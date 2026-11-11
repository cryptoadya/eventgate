"""Intentionally faulty demo handler: a repeated request creates another payment."""

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from eventgate.contracts.payment import PaymentRequested
from eventgate_demo.db import insert_inbox, insert_payment_and_outbox


async def process_buggy(
    connection: AsyncConnection[tuple[object, ...]], request: PaymentRequested
) -> None:
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("Payment processing requires an idle PostgreSQL connection")
    async with connection.transaction():
        # Deliberate defect: the existing inbox record should prevent another payment.
        await insert_inbox(connection, request.event_id)
        await insert_payment_and_outbox(connection, request)
