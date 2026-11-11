import time

import psycopg
import pytest

from eventgate.config import VerifyConfig
from eventgate.contracts.payment import PaymentRequested
from eventgate.environment import EnvironmentHandle, EnvironmentManager
from eventgate.kafka_io import KafkaIO, OutboundMessage

REQUEST = PaymentRequested(
    schema_version=1, event_id="evt-1001", order_id="order-1042", amount_minor=5000, currency="EUR"
)


@pytest.mark.parametrize(("variant", "duplicate_count"), [("stable", 1), ("buggy", 2)])
def test_inbox_controls_payment_count_without_changing_schema(
    integration_config: VerifyConfig,
    environments: tuple[EnvironmentManager, list[EnvironmentHandle]],
    request: pytest.FixtureRequest,
    variant: str,
    duplicate_count: int,
) -> None:
    manager, handles = environments
    image = request.getfixturevalue(f"{variant}_image")
    handle = manager.create(integration_config, "baseline", "duplicate_delivery")
    handles.append(handle)
    manager.start_infrastructure(handle)
    manager.start_application(handle, image_id=image)

    kafka = KafkaIO(handle.kafka_bootstrap)
    topic = integration_config.kafka.input_topic
    outbound = OutboundMessage(REQUEST.event_id.encode(), REQUEST.model_dump_json().encode())
    deadline = time.monotonic() + 25
    for expected_offset, expected_count in enumerate((1, duplicate_count)):
        [sent] = kafka.send_confirmed(topic, [outbound], deadline)
        assert sent.offset == expected_offset
        while time.monotonic() < deadline:
            position = kafka.committed_offsets(handle.consumer_group, topic, deadline)
            if position == {0: expected_offset + 1}:
                break
            time.sleep(0.1)
        else:
            pytest.fail("Consumer did not commit the input")
        with psycopg.connect(handle.postgres_dsn) as conn:
            counts = conn.execute(
                "SELECT (SELECT count(*) FROM processed_events), "
                "(SELECT count(*) FROM payments), (SELECT count(*) FROM outbox)"
            ).fetchone()
        assert counts == (1, expected_count, 1)
