"""Shared PostgreSQL schema and transaction operations."""

from collections.abc import Callable

from psycopg import AsyncConnection, Connection
from psycopg.types.json import Jsonb
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Identity,
    MetaData,
    Table,
    Text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Dialect
from sqlalchemy.schema import CreateTable

from eventgate.contracts.payment import PaymentRequested, make_completion

# SQLAlchemy exposes a dialect factory whose constructor has no type annotations.
_postgres_dialect: Callable[[], Dialect] = postgresql.dialect

SCHEMA = MetaData(schema="public")
Table(
    "processed_events",
    SCHEMA,
    Column("event_id", Text, primary_key=True),
)
Table(
    "payments",
    SCHEMA,
    Column("payment_id", BigInteger, Identity(always=True), primary_key=True),
    Column("event_id", Text, ForeignKey("public.processed_events.event_id"), nullable=False),
    Column("order_id", Text, nullable=False),
    Column("amount_minor", BigInteger, nullable=False),
    Column("currency", Text, nullable=False),
    CheckConstraint("amount_minor > 0", name="payments_positive_amount"),
    CheckConstraint("currency = 'EUR'", name="payments_eur"),
)
Table(
    "outbox",
    SCHEMA,
    Column("completion_id", Text, primary_key=True),
    Column(
        "source_event_id",
        Text,
        ForeignKey("public.processed_events.event_id"),
        unique=True,
        nullable=False,
    ),
    Column("payload", JSONB, nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
)


def init_db(connection: Connection[tuple[object, ...]]) -> None:
    with connection.transaction():
        for table in SCHEMA.sorted_tables:
            connection.execute(str(CreateTable(table).compile(dialect=_postgres_dialect())))


async def insert_inbox(connection: AsyncConnection[tuple[object, ...]], event_id: str) -> bool:
    cursor = await connection.execute(
        "INSERT INTO public.processed_events (event_id) VALUES (%s) "
        "ON CONFLICT DO NOTHING RETURNING event_id",
        (event_id,),
    )
    return await cursor.fetchone() is not None


async def insert_payment_and_outbox(
    connection: AsyncConnection[tuple[object, ...]], request: PaymentRequested
) -> None:
    await connection.execute(
        "INSERT INTO public.payments (event_id, order_id, amount_minor, currency) VALUES (%s, %s, %s, %s)",
        (request.event_id, request.order_id, request.amount_minor, request.currency),
    )
    completion = make_completion(request)
    await connection.execute(
        "INSERT INTO public.outbox (completion_id, source_event_id, payload) VALUES (%s, %s, %s) "
        "ON CONFLICT DO NOTHING",
        (completion.completion_id, request.event_id, Jsonb(completion.model_dump(mode="json"))),
    )
