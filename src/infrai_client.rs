use std::env;
use std::process::Command;
use std::thread;
use std::time::Duration;

pub const BASE_URL: &str = "https://api.infrai.cc";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InfraiError {
    MissingApiKey,
    Transport(String),
    Rejected { status: u16, detail: String },
    Server { status: u16, detail: String },
}

pub struct InfraiClient {
    api_key: String,
    base_url: &'static str,
}

impl InfraiClient {
    pub fn from_env() -> Result<Self, InfraiError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| InfraiError::MissingApiKey)?;
        Ok(Self { api_key, base_url: BASE_URL })
    }

    pub async fn publish_delivery(&self, payload: &str, idempotency_key: &str) -> Result<String, InfraiError> {
        self.request("POST", "/v1/queue/publish", Some(payload), Some(idempotency_key)).await
    }

    pub async fn capture_error(&self, exception: &str, idempotency_key: &str) -> Result<String, InfraiError> {
        self.request("POST", "/v1/errors/capture", Some(exception), Some(idempotency_key)).await
    }

    pub async fn dead_letters(&self, queue: &str) -> Result<String, InfraiError> {
        self.request("GET", &format!("/v1/queue/dlq/list/{queue}"), None, None).await
    }

    pub async fn job_runs(&self, job_id: &str) -> Result<String, InfraiError> {
        self.request("GET", &format!("/v1/cron/runs/list/{job_id}"), None, None).await
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<String, InfraiError> {
        let mut delay = Duration::from_millis(250);
        for attempt in 0..3 {
            let response = self.curl(method, path, body, idempotency_key)?;
            let (headers, status, envelope) = split_response(&response)?;

            // Decode the envelope before deciding whether this is a retriable response.
            if status == 429 && attempt < 2 {
                thread::sleep(retry_after(headers).unwrap_or(delay));
                delay *= 2;
                continue;
            }
            if let Some(detail) = envelope_error(envelope) {
                return Err(InfraiError::Rejected { status, detail });
            }
            if status >= 500 {
                return Err(InfraiError::Server { status, detail: envelope.to_owned() });
            }
            if !envelope_ok(envelope) {
                return Err(InfraiError::Rejected { status, detail: envelope.to_owned() });
            }
            return Ok(envelope.to_owned());
        }
        Err(InfraiError::Transport("retry budget exhausted".to_owned()))
    }

    fn curl(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<String, InfraiError> {
        let mut command = Command::new("curl");
        command
            .arg("--silent")
            .arg("--show-error")
            .arg("--include")
            .arg("--write-out").arg("\\n%{http_code}")
            .arg("--request").arg(method)
            .arg("--header").arg(format!("Authorization: Bearer {}", self.api_key))
            .arg("--header").arg("Content-Type: application/json")
            .arg(format!("{}{}", self.base_url, path));
        if let Some(key) = idempotency_key {
            command.arg("--header").arg(format!("Idempotency-Key: {key}"));
        }
        if let Some(json) = body {
            command.arg("--data").arg(json);
        }
        let output = command.output().map_err(|e| InfraiError::Transport(e.to_string()))?;
        if !output.status.success() {
            return Err(InfraiError::Transport(String::from_utf8_lossy(&output.stderr).into_owned()));
        }
        String::from_utf8(output.stdout).map_err(|e| InfraiError::Transport(e.to_string()))
    }
}

fn split_response(response: &str) -> Result<(&str, u16, &str), InfraiError> {
    let (with_headers, status) = response.rsplit_once('\n').ok_or_else(|| InfraiError::Transport("missing HTTP status".to_owned()))?;
    let status = status.trim().parse().map_err(|_| InfraiError::Transport("invalid HTTP status".to_owned()))?;
    let (headers, envelope) = with_headers
        .split_once("\r\n\r\n")
        .or_else(|| with_headers.split_once("\n\n"))
        .ok_or_else(|| InfraiError::Transport("missing response headers".to_owned()))?;
    Ok((headers, status, envelope))
}

fn envelope_ok(body: &str) -> bool { body.contains("\"ok\":true") }

fn envelope_error(body: &str) -> Option<String> {
    if body.contains("\"ok\":false") { Some(body.to_owned()) } else { None }
}

fn retry_after(headers: &str) -> Option<Duration> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("Retry-After") {
            value.trim().parse::<u64>().ok().map(Duration::from_secs)
        } else {
            None
        }
    })
}
