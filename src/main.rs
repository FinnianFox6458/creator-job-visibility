mod creator_delivery;
mod infrai_client;

use std::env;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use creator_delivery::{record_processing_failure, ProcessingFailure};
use infrai_client::InfraiClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = InfraiClient::from_env().map_err(|error| format!("client setup: {error:?}"))?;
    let job_id = env::var("INFRAI_JOB_ID").map_err(|_| "set INFRAI_JOB_ID to the content-processing cron job")?;
    let failure = ProcessingFailure {
        asset_id: "lesson-202".to_owned(),
        subscriber_id: "subscriber-18".to_owned(),
        stage: "digital-asset-processing".to_owned(),
    };
    block_on(record_processing_failure(&client, &failure)).map_err(|error| format!("record failure: {error:?}"))?;
    let dead_letters = block_on(client.dead_letters("creator-delivery"))
        .map_err(|error| format!("read dead letters: {error:?}"))?;
    let runs = block_on(client.job_runs(&job_id))
        .map_err(|error| format!("read job runs: {error:?}"))?;
    println!("delivery failure captured; runs: {runs}; dead-letter view: {dead_letters}");
    Ok(())
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

unsafe fn clone(_: *const ()) -> RawWaker { noop_raw_waker() }
unsafe fn wake(_: *const ()) {}
unsafe fn wake_by_ref(_: *const ()) {}
unsafe fn drop_waker(_: *const ()) {}

fn noop_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &RawWakerVTable::new(clone, wake, wake_by_ref, drop_waker))
}
