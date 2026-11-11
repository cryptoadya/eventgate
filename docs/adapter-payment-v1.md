# The payment-v1 adapter contract

An application image exposes `init-db`, `consumer`, and `publisher` commands.
The runner uses the same image for all three. The runner supplies the database URL,
Kafka address, input/output topics, consumer group, operation timeout, and test control
settings through environment variables. See the packaged Compose resource for the
exact environment variable names. The input and output topics have one partition.

An input is a UTF-8 JSON object with a strict integer `schema_version: 1`, an
`event_id`, an `order_id`, a positive signed 64-bit integer `amount_minor`, and
`currency: "EUR"`. Request and order IDs use ASCII letters, digits, underscores,
and hyphens (1–128 characters). The Kafka key equals the event ID. The output carries
`schema_version`, `completion_id` (for example `payment.completed:evt-1001`),
`source_event_id`, `order_id`, `amount_minor`, and `currency`. The Kafka key equals
the completion ID. Additional output fields are allowed and compared unless explicitly
allowlisted across versions. Additional fields must match between physical copies and
their outbox payload inside each version.

The required PostgreSQL tables in `public` are:

| Table | Contract |
| --- | --- |
| `processed_events` | `event_id TEXT PRIMARY KEY` |
| `payments` | `payment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`; non-null `event_id`, `order_id`, `amount_minor`, `currency`; positive amount and EUR checks; foreign key to the inbox; no unique constraint on `event_id` |
| `outbox` | `completion_id TEXT PRIMARY KEY`; unique non-null `source_event_id` referencing the inbox; `payload JSONB NOT NULL`; nullable `published_at TIMESTAMPTZ` |

## Test control

Enable the test API with `EVENTGATE_TEST_MODE=1`. Ports are published to host loopback
only. Each process reports its contract, role, state, run, scenario, instance UUID,
and verified Kafka/PostgreSQL dependency status.

| Request | Result |
| --- | --- |
| `GET /test/status` | Current status; the consumer is ready only after partition assignment |
| `POST /test/pause` with `{}` | Confirms that the worker is paused and has no in-flight operation |
| `POST /test/resume` with `{}` | Confirms that the worker can accept work |
| `POST /test/failpoints/arm` | Durably arms a UUID `token` for the named stage and `target_id` |

Consumer stage: `after_db_commit_before_offset_commit`, targeted by `event_id`.
Publisher stage: `after_publish_ack_before_outbox_mark`, targeted by `completion_id`.
A fired point writes a receipt to `/var/lib/eventgate-control/failpoint.json` before
exiting with code 86. The same token does not fire again after restart. Other
application failures use nonzero exit codes distinct from 86. The runner verifies
the receipt, exit, and independent Kafka/PostgreSQL state.

The consumer logs `message_received` as a JSON line with topic, partition, offset,
event ID, and process instance ID. The runner uses those fields to confirm the same
physical input was read by the restarted process. No business-effect totals are
accepted from the worker API.
