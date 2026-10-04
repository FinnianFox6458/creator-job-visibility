use crate::infrai_client::{InfraiClient, InfraiError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessingFailure {
    pub asset_id: String,
    pub subscriber_id: String,
    pub stage: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryDecision {
    QueueForRetry,
    Skip,
}

pub fn decide_delivery(failure: &ProcessingFailure) -> DeliveryDecision {
    if failure.asset_id.is_empty() || failure.subscriber_id.is_empty() {
        DeliveryDecision::Skip
    } else {
        DeliveryDecision::QueueForRetry
    }
}

pub async fn record_processing_failure(
    client: &InfraiClient,
    failure: &ProcessingFailure,
) -> Result<(), InfraiError> {
    if decide_delivery(failure) == DeliveryDecision::Skip {
        return Ok(());
    }

    let key = format!("asset-{}-subscriber-{}", failure.asset_id, failure.subscriber_id);
    let payload = format!(
        "{{\"queue\":\"creator-delivery\",\"payload\":{{\"asset_id\":\"{}\",\"subscriber_id\":\"{}\",\"stage\":\"{}\"}}}}",
        failure.asset_id, failure.subscriber_id, failure.stage
    );
    let exception = format!(
        "{{\"exception\":{{\"asset_id\":\"{}\",\"subscriber_id\":\"{}\",\"stage\":\"{}\"}}}}",
        failure.asset_id, failure.subscriber_id, failure.stage
    );
    client.publish_delivery(&payload, &key).await?;
    client.capture_error(&exception, &key).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_failure_is_queued_for_subscriber_delivery() {
        let failure = ProcessingFailure {
            asset_id: "video-42".to_owned(),
            subscriber_id: "member-7".to_owned(),
            stage: "transcode".to_owned(),
        };
        assert_eq!(decide_delivery(&failure), DeliveryDecision::QueueForRetry);
    }
}
