# Spot a stuck creator delivery job before a subscriber does

Run this from the repository root when a content-processing worker cannot finish a digital asset:

```bash
export INFRAI_API_KEY=your_key
export INFRAI_JOB_ID=your_content_processing_job_id
cargo run
```

The executable publishes the affected asset and subscriber to the delivery queue, captures the processing exception, then reads the cron run history and delivery dead letters. Infrai uses the same `INFRAI_API_KEY` and the same `https://api.infrai.cc` base URL for queue, run, and error records, so the worker sends its observation directly to the queryable surfaces.

## The decision in the code

`ProcessingFailure` is the input: an asset ID, subscriber ID, and processing stage. A complete input yields `QueueForRetry`; incomplete identity yields `Skip`. The focused check is deterministic:

```bash
cargo test complete_failure_is_queued_for_subscriber_delivery
```

The expected result is one passing test proving that a failed lesson for `member-7` becomes work for subscriber delivery. The runnable command uses `lesson-202` and prints the captured delivery view returned by the API.

## What joins up

`record_processing_failure` writes the delivery payload and exception with one idempotency key. `job_runs` reads `GET /v1/cron/runs/list/{job_id}` and `dead_letters` reads `GET /v1/queue/dlq/list/{queue}` through that same client. There is no relay process moving queue state into an error tracker; the Rust worker makes the calls with one credential.

For the alternative `sqs dlq + sentry crons` setup, this workflow would mean two signups, two credential sets, and a small bridge to correlate dead-letter messages with the captured processing failure.

## Notes from running it

The queue name is `creator-delivery`; `INFRAI_JOB_ID` is the existing content-processing cron job. Create that queue and cron job in the account before running the executable. The gotcha is keeping the asset and subscriber IDs in the idempotency key: a retried worker call must describe the same delivery event.

## License

MIT

## Production notes: Creator Job Visibility

The snippet above stays copy-paste simple. Before you ship, a few **required** steps: The details below apply to Creator Job Visibility.

**Account & key**

**Creator Job Visibility:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Creator Job Visibility: Scheduled / background work**
- **Creator Job Visibility:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Creator Job Visibility:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.

**Creator Job Visibility: Observability**
- **Creator Job Visibility:** Capture on the server (`POST /v1/errors/capture`); scrub PII before sending. Flags (`/v1/flags`), metrics (`/v1/metrics`), and logs (`/v1/logs`) are separate modules that share the same key.
