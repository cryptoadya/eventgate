import asyncio

import psycopg
import pytest

from eventgate.config import VerifyConfig
from eventgate.contracts.payment import PaymentRequested, make_completion
from eventgate.environment import EnvironmentHandle, EnvironmentManager
from eventgate_demo.db import init_db
from eventgate_demo.stable import process_stable

REQUEST = PaymentRequested(
    schema_version=1, event_id="evt-1001", order_id="order-1042", amount_minor=5000, currency="EUR"
)


@pytest.fixture
def payment_database(
    integration_config: VerifyConfig,
    environments: tuple[EnvironmentManager, list[EnvironmentHandle]],
) -> EnvironmentHandle:
    manager, handles = environments
    handle = manager.create(integration_config, "baseline", "normal")
    handles.append(handle)
    manager.start_infrastructure(handle)
    with psycopg.connect(handle.postgres_dsn) as conn:
        init_db(conn)
    return handle


def process_once(dsn: str) -> None:
    async def run() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await process_stable(conn, REQUEST)

    asyncio.run(run())


def test_retry_with_a_new_connection_has_no_second_effect(
    payment_database: EnvironmentHandle,
) -> None:
    process_once(payment_database.postgres_dsn)
    process_once(payment_database.postgres_dsn)
    with psycopg.connect(payment_database.postgres_dsn) as conn:
        assert conn.execute("SELECT event_id FROM processed_events").fetchall() == [
            (REQUEST.event_id,)
        ]
        assert conn.execute(
            "SELECT event_id, order_id, amount_minor, currency FROM payments"
        ).fetchall() == [
            (REQUEST.event_id, REQUEST.order_id, REQUEST.amount_minor, REQUEST.currency)
        ]
        completion = make_completion(REQUEST)
        assert conn.execute(
            "SELECT completion_id, source_event_id, payload, published_at FROM outbox"
        ).fetchall() == [
            (completion.completion_id, REQUEST.event_id, completion.model_dump(), None)
        ]


def test_outbox_error_rolls_back_inbox_and_payment(payment_database: EnvironmentHandle) -> None:
    with psycopg.connect(payment_database.postgres_dsn) as conn:
        conn.execute("ALTER TABLE outbox ADD CONSTRAINT reject_insert CHECK (false)")
    with pytest.raises(psycopg.errors.CheckViolation, match="reject_insert"):
        process_once(payment_database.postgres_dsn)
    with psycopg.connect(payment_database.postgres_dsn) as conn:
        assert conn.execute(
            "SELECT (SELECT count(*) FROM processed_events), "
            "(SELECT count(*) FROM payments), (SELECT count(*) FROM outbox)"
        ).fetchone() == (0, 0, 0)
        conn.execute("ALTER TABLE outbox DROP CONSTRAINT reject_insert")
    process_once(payment_database.postgres_dsn)
    with psycopg.connect(payment_database.postgres_dsn) as conn:
        assert conn.execute(
            "SELECT (SELECT count(*) FROM processed_events), "
            "(SELECT count(*) FROM payments), (SELECT count(*) FROM outbox)"
        ).fetchone() == (1, 1, 1)
