# Payment demo

The demo uses synthetic requests and never handles real money. Stable and Buggy use the
same image definition, database schema, and publisher. Buggy intentionally ignores the
result of inserting the inbox marker, so it creates a second payment for the same
request. Its outbox still contains one notification.

## Build and prepare

From the repository root, install the development environment from the README. With
Docker Engine and Compose available, run:

```bash
set -a
source config/infrastructure.env.example
set +a
docker pull "$EVENTGATE_KAFKA_IMAGE"
docker pull "$EVENTGATE_POSTGRES_IMAGE"
docker build -f docker/payment.Dockerfile --build-arg PAYMENT_VARIANT=stable -t eventgate-payment:stable .
docker build -f docker/payment.Dockerfile --build-arg PAYMENT_VARIANT=buggy -t eventgate-payment:buggy .
export EVENTGATE_BASELINE_IMAGE=eventgate-payment:stable
export EVENTGATE_STABLE_IMAGE=eventgate-payment:stable
export EVENTGATE_BUGGY_IMAGE=eventgate-payment:buggy
```

The tags are local names. Without source metadata supplied at build time, the image
revision is marked unknown.

## Observe the defect

The following uses the same input twice. It waits for each committed Kafka offset
before querying PostgreSQL. Each version receives its own fresh database and broker.

```bash
.venv/bin/python - <<'PY'
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import psycopg

from eventgate.config import load_config
from eventgate.contracts.payment import PaymentRequested
from eventgate.environment import EnvironmentManager
from eventgate.kafka_io import KafkaIO, OutboundMessage

config = load_config(Path("examples/stable-vs-buggy.yaml"))
manager = EnvironmentManager(f"demo-{uuid4().hex}")
request = PaymentRequested(
    schema_version=1, event_id="evt-1001", order_id="order-1042", amount_minor=5000, currency="EUR"
)
message = OutboundMessage(request.event_id.encode(), request.model_dump_json().encode())
for version in ("baseline", "candidate"):
    env = manager.create(config, version, "duplicate_delivery")
    try:
        manager.start_infrastructure(env)
        manager.start_application(env)
        kafka = KafkaIO(env.kafka_bootstrap)
        deadline = monotonic() + config.timeouts.progress_seconds
        for _ in range(2):
            topic = config.kafka.input_topic
            [receipt] = kafka.send_confirmed(topic, [message], deadline)
            while kafka.committed_offsets(env.consumer_group, topic, deadline) != {0: receipt.offset + 1}:
                sleep(0.1)
            with psycopg.connect(env.postgres_dsn, connect_timeout=5) as conn:
                counts = conn.execute(
                    "SELECT (SELECT count(*) FROM processed_events), "
                    "(SELECT count(*) FROM payments), (SELECT count(*) FROM outbox)"
                ).fetchone()
            print(version, "input offset:", receipt.offset, "inbox/payment/outbox:", counts)
    finally:
        manager.destroy(env)
PY
```

Expected: both versions report `(1, 1, 1)` after offset 0. After offset 1, Stable
still reports `(1, 1, 1)`, while Buggy reports `(1, 2, 1)`.

## Verify all five scenarios

| Scenario | What the runner observes |
| --- | --- |
| `normal` | One request produces one payment and one logical completion. |
| `duplicate_delivery` | Two input positions carry the same request. Stable still has one payment. |
| `consumer_restart` | The consumer exits cleanly and restarts; its previous payment survives. |
| `crash_after_db_commit` | The payment exists before the offset is committed. Kafka delivers the same physical request after restart. |
| `crash_after_publish_ack` | The completion is already visible while the outbox is pending. The publisher sends another copy after restart. |

Run `.venv/bin/eventgate verify --config examples/stable-vs-buggy.yaml`. The local
Buggy image should fail `duplicate_delivery` and `crash_after_db_commit`. Read the
candidate's `payment_count` check to see the unexpected second payment. The publisher
crash is expected to pass: duplicate completion messages are allowed when their
identity and payload agree.

This timeline excerpt comes from a local ARM64 run of the Buggy consumer crash on
2026-11-11 (UTC). The offset stayed at 0 for the two application observations:

| Time (UTC) | Source | Observation |
| --- | --- | --- |
| 16:11:10.601 | Runner | Input acknowledged at partition 0, offset 0 |
| 16:11:10.619 | Application | First instance read offset 0 |
| 16:11:10.625 | Application | First instance committed PostgreSQL |
| 16:11:10.827 | Runner | Observed process exit 86 and durable receipt |
| 16:11:16.607 | Application | New instance read offset 0 |
| 16:11:17.604 | Runner | Confirmed new instance readiness |
| 16:11:17.981 | Runner | Collected final snapshot with two payment rows |

Runner timestamps reflect when the runner observed an event. An application log can
show that a restarted worker received an input before the next readiness response.

## Verified local run

On 2026-11-11 a clean source copy on macOS ARM64 passed installation and both demo
runs. The host used Python 3.12.0, Docker client 29.9.3, Docker Engine 29.5.2 on ARM64,
and Compose 2.40.3. The demo images used Python 3.12.16.

| Comparison | Exit and verdict | CLI wall time, including cleanup |
| --- | --- | ---: |
| Stable / Stable | 0, PASS | 159.17 s |
| Stable / Buggy | 1, expected FAIL | 158.70 s |

The two image pull commands took 44.21 s and 43.83 s with images already cached.
The Stable and Buggy build commands took 37.59 s and 31.62 s. These times are
separate from verification. Native Linux x86-64 and GitHub-hosted performance
remain unverified. See [CI and the demo gate](ci.md) for the remaining hosted checks.
