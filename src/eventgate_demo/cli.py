"""Commands used to run the synthetic payment application."""

import argparse
import asyncio
import math
import os
import signal
import sys
from contextlib import nullcontext
from pathlib import Path
from threading import Event
from uuid import uuid4

import psycopg

from eventgate.config import SCENARIO_ORDER
from eventgate.contracts.control import Role
from eventgate_demo.buggy import process_buggy
from eventgate_demo.consumer import log_event, run_consumer
from eventgate_demo.control import ControlPlane, serve_control
from eventgate_demo.db import init_db
from eventgate_demo.failpoints import FailpointStore
from eventgate_demo.publisher import run_publisher
from eventgate_demo.stable import process_stable


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _operation_seconds() -> float:
    try:
        value = float(_required("EVENTGATE_OPERATION_TIMEOUT_SECONDS"))
    except ValueError:
        raise ValueError("EVENTGATE_OPERATION_TIMEOUT_SECONDS must be a positive number") from None
    if value <= 0 or not math.isfinite(value):
        raise ValueError("EVENTGATE_OPERATION_TIMEOUT_SECONDS must be a positive number")
    return value


def _control_plane(
    role: Role, instance: str, bootstrap: str, topic: str, seconds: float
) -> ControlPlane | None:
    if os.environ.get("EVENTGATE_TEST_MODE") != "1":
        return None
    scenario_name = _required("EVENTGATE_SCENARIO_ID")
    scenario = next((item for item in SCENARIO_ORDER if item == scenario_name), None)
    if scenario is None:
        raise ValueError("Unknown EVENTGATE_SCENARIO_ID")
    store = FailpointStore(
        Path(_required("EVENTGATE_CONTROL_DIR")) / "failpoint.json",
        _required("EVENTGATE_RUN_ID"),
        scenario,
        role,
        instance,
    )
    return ControlPlane(store, bootstrap, topic, seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the synthetic payment-v1 application.")
    parser.add_argument("command", choices=["init-db", "consumer", "publisher"])
    command = parser.parse_args().command

    instance = str(uuid4())
    try:
        dsn = _required("DATABASE_URL")
        timeout = _operation_seconds()
        if command == "init-db":
            with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as connection:
                init_db(connection)
            return 0
        stop = Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        bootstrap = _required("KAFKA_BOOTSTRAP_SERVERS")
        role: Role = "consumer" if command == "consumer" else "publisher"
        topic = _required("INPUT_TOPIC" if role == "consumer" else "OUTPUT_TOPIC")
        control = _control_plane(role, instance, bootstrap, topic, timeout)
        with serve_control(control) if control is not None else nullcontext():
            if role == "consumer":
                handlers = {"stable": process_stable, "buggy": process_buggy}
                handler = handlers.get(os.environ.get("PAYMENT_VARIANT", "stable"))
                if handler is None:
                    raise ValueError("Unknown PAYMENT_VARIANT")
                asyncio.run(
                    run_consumer(
                        bootstrap,
                        dsn,
                        topic,
                        _required("CONSUMER_GROUP_ID"),
                        timeout,
                        stop,
                        process_instance_id=instance,
                        handler=handler,
                        control=control,
                    )
                )
            else:
                asyncio.run(
                    run_publisher(
                        bootstrap,
                        dsn,
                        topic,
                        timeout,
                        stop,
                        process_instance_id=instance,
                        control=control,
                    )
                )
    except Exception as error:
        log_event("worker_error", instance, role=command, error_type=type(error).__name__)
        print(f"{command} failed: {type(error).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
