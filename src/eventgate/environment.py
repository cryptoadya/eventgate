"""Create and clean up a private Compose project for a scenario."""

import asyncio
import base64
import errno
import json
import os
import re
import secrets
import socket
import subprocess
import time
from dataclasses import dataclass, field
from importlib.resources import as_file, files
from typing import Literal
from uuid import uuid4

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition

# NewTopic is exported at runtime but is not an explicit re-export in the client annotations.
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from eventgate.config import ScenarioID, VerifyConfig
from eventgate.contracts.control import Role

_VOLUME_NAMES = (
    "kafka-data",
    "kafka-secrets",
    "kafka-config",
    "postgres-data",
    "consumer-control",
    "publisher-control",
)
_RESOURCE_KINDS = ("container", "network", "volume")
_BIND_ERROR = re.compile(
    r"address already in use|port is already allocated|port is already in use|ports? are not available",
    re.IGNORECASE,
)
_MAX_BIND_RETRIES = 3


class EnvironmentError(RuntimeError):
    """The environment could not be prepared or inspected."""


class DockerError(EnvironmentError):
    def __init__(self, message: str, *, stderr: str = "") -> None:
        self.stderr = stderr
        super().__init__(message)


class CleanupError(EnvironmentError):
    def __init__(self, remaining: tuple[str, ...], errors: tuple[str, ...]) -> None:
        self.remaining = remaining
        self.errors = errors
        super().__init__(
            "Could not verify removal of all environment resources: " + ", ".join(remaining)
        )


@dataclass
class EnvironmentHandle:
    config: VerifyConfig = field(repr=False)
    run_id: str
    environment_id: str
    project: str
    version: Literal["baseline", "candidate"]
    scenario_id: ScenarioID
    username: str = field(repr=False)
    password: str = field(repr=False)
    cluster_id: str
    health_topic: str
    app_image: str
    kafka_port: int = 0
    postgres_port: int = 0
    attempted: bool = False
    control_ports: dict[Role, int] = field(default_factory=dict)
    startup_diagnostics: list[str] = field(default_factory=list, repr=False)

    @property
    def volumes(self) -> dict[str, str]:
        return {name: f"{self.project}_{name}" for name in _VOLUME_NAMES}

    @property
    def consumer_group(self) -> str:
        return f"{self.project}-consumer"

    @property
    def kafka_bootstrap(self) -> str:
        return f"127.0.0.1:{self.kafka_port}"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.username}:{self.password}@127.0.0.1:{self.postgres_port}/eventgate"
        )


@dataclass(frozen=True)
class ContainerState:
    running: bool
    exit_code: int


def remaining(deadline: float) -> float:
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError("Environment operation reached its deadline")
    return seconds


class EnvironmentManager:
    def __init__(self, run_id: str) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id):
            raise ValueError("Invalid run identifier")
        self.run_id = run_id

    def create(
        self,
        config: VerifyConfig,
        version: Literal["baseline", "candidate"],
        scenario_id: ScenarioID,
    ) -> EnvironmentHandle:
        while True:
            environment_id = uuid4().hex
            health_topic = f"eventgate.health.{environment_id}"
            if health_topic not in {config.kafka.input_topic, config.kafka.output_topic}:
                break
        return EnvironmentHandle(
            config=config,
            run_id=self.run_id,
            environment_id=environment_id,
            project=f"eventgate-{environment_id}",
            version=version,
            scenario_id=scenario_id,
            username=f"eg_{secrets.token_hex(8)}",
            password=secrets.token_hex(24),
            cluster_id=base64.urlsafe_b64encode(uuid4().bytes).decode().rstrip("="),
            health_topic=health_topic,
            app_image=(config.baseline if version == "baseline" else config.candidate).image,
        )

    @staticmethod
    def _allocate_ports() -> tuple[int, int]:
        with socket.socket() as kafka, socket.socket() as postgres:
            kafka.bind(("127.0.0.1", 0))
            postgres.bind(("127.0.0.1", 0))
            return int(kafka.getsockname()[1]), int(postgres.getsockname()[1])

    @staticmethod
    def _docker(
        *args: str,
        deadline: float,
        environment: dict[str, str] | None = None,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        try:
            result = subprocess.run(
                ["docker", *args],
                env=environment,
                input=input_text,
                capture_output=True,
                text=True,
                errors="replace",
                timeout=remaining(deadline),
                check=False,
            )
        except FileNotFoundError:
            raise DockerError("Docker CLI is not installed") from None
        except (OSError, TimeoutError, subprocess.TimeoutExpired) as error:
            raise DockerError(f"Docker command could not finish: {type(error).__name__}") from None
        if result.returncode:
            raise DockerError(f"Docker command exited {result.returncode}", stderr=result.stderr)
        return result

    def _compose(
        self,
        handle: EnvironmentHandle,
        *args: str,
        deadline: float,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = {
            key: value for key, value in os.environ.items() if not key.startswith("COMPOSE_")
        }
        environment.update(
            EG_RUN_ID=handle.run_id,
            EG_SCENARIO_ID=handle.scenario_id,
            EG_ENVIRONMENT_ID=handle.environment_id,
            EG_KAFKA_IMAGE=handle.config.infrastructure.kafka_image,
            EG_POSTGRES_IMAGE=handle.config.infrastructure.postgres_image,
            EG_KAFKA_PORT=str(handle.kafka_port),
            EG_POSTGRES_PORT=str(handle.postgres_port),
            EG_POSTGRES_USER=handle.username,
            EG_POSTGRES_PASSWORD=handle.password,
            EG_CLUSTER_ID=handle.cluster_id,
            EG_APP_IMAGE=handle.app_image,
            EG_APP_DSN=f"postgresql://{handle.username}:{handle.password}@postgres:5432/eventgate",
            EG_INPUT_TOPIC=handle.config.kafka.input_topic,
            EG_OUTPUT_TOPIC=handle.config.kafka.output_topic,
            EG_CONSUMER_GROUP=handle.consumer_group,
            EG_OPERATION_SECONDS=str(handle.config.timeouts.control_seconds / 2),
        )
        resource = files("eventgate").joinpath("resources", "compose.yaml")
        with as_file(resource) as path:
            return self._docker(
                "compose",
                "--project-name",
                handle.project,
                "--env-file",
                os.devnull,
                "--file",
                str(path),
                *args,
                deadline=deadline,
                environment=environment,
                input_text=input_text,
            )

    def start_infrastructure(self, handle: EnvironmentHandle) -> None:
        if handle.run_id != self.run_id or handle.attempted:
            raise EnvironmentError("Expected an unused environment owned by this run")
        deadline = time.monotonic() + handle.config.timeouts.startup_seconds
        for attempt in range(_MAX_BIND_RETRIES + 1):
            handle.kafka_port, handle.postgres_port = self._allocate_ports()
            try:
                self._check_host_ports(handle)
                handle.attempted = True
                self._compose(
                    handle, "up", "-d", "--pull", "never", "kafka", "postgres", deadline=deadline
                )
            except DockerError as error:
                if not _BIND_ERROR.search(error.stderr):
                    raise
                if handle.attempted:
                    try:
                        logs = self.collect_logs(handle)
                        if logs:
                            handle.startup_diagnostics[:] = [logs]
                    except DockerError as log_error:
                        handle.startup_diagnostics.append(
                            f"Could not collect startup logs: {log_error}"
                        )
                handle.startup_diagnostics.append(error.stderr.strip())
                self.destroy(handle)
                if attempt == _MAX_BIND_RETRIES:
                    raise DockerError(
                        "Docker port bind failed after three retries", stderr=error.stderr
                    ) from None
                continue
            break
        self._wait_postgres(handle, deadline)
        self._prepare_kafka(handle, deadline)

    @staticmethod
    def _check_host_ports(handle: EnvironmentHandle) -> None:
        # On macOS the Engine runs in a VM; a guest bind can succeed when a host port is busy.
        try:
            with socket.socket() as kafka, socket.socket() as postgres:
                kafka.bind(("127.0.0.1", handle.kafka_port))
                postgres.bind(("127.0.0.1", handle.postgres_port))
        except OSError as error:
            if error.errno == errno.EADDRINUSE:
                raise DockerError(
                    "Docker port bind failed", stderr="address already in use"
                ) from None
            raise EnvironmentError("Cannot check local loopback ports") from error

    @staticmethod
    def _wait_postgres(handle: EnvironmentHandle, deadline: float) -> None:
        async def query() -> bool:
            async with await psycopg.AsyncConnection.connect(
                handle.postgres_dsn, autocommit=True, options="-c statement_timeout=500"
            ) as conn:
                cursor = await conn.execute("SELECT 1, current_user, current_database()")
                return await cursor.fetchone() == (1, handle.username, "eventgate")

        async def bounded_query(timeout: float) -> bool:
            return await asyncio.wait_for(query(), timeout=timeout)

        delay = 0.1
        while deadline > time.monotonic():
            try:
                if asyncio.run(bounded_query(min(2, remaining(deadline)))):
                    return
            except (psycopg.Error, TimeoutError):
                pass
            time.sleep(min(delay, max(0, deadline - time.monotonic())))
            delay = min(delay * 2, 1)
        raise EnvironmentError("PostgreSQL did not accept a test query within the startup deadline")

    def _prepare_kafka(self, handle: EnvironmentHandle, deadline: float) -> None:
        conf: dict[str, str | int | float | bool] = {
            "bootstrap.servers": handle.kafka_bootstrap,
            "log_level": 0,
        }
        admin = AdminClient(conf)
        delay = 0.1
        while True:
            try:
                metadata = admin.list_topics(timeout=min(remaining(deadline), 2))
                if metadata.brokers:
                    if metadata.cluster_id != handle.cluster_id:
                        raise EnvironmentError("Kafka listener advertised a different cluster")
                    break
            except KafkaException:
                pass
            time.sleep(min(delay, remaining(deadline)))
            delay = min(delay * 2, 1)

        topics = (
            handle.config.kafka.input_topic,
            handle.config.kafka.output_topic,
            handle.health_topic,
        )
        requests = [
            NewTopic(
                topic,
                num_partitions=1,
                replication_factor=1,
                config={"cleanup.policy": "delete", "retention.ms": "-1"},
            )
            for topic in topics
        ]
        futures = admin.create_topics(requests, request_timeout=remaining(deadline))
        for future in futures.values():
            future.result(timeout=remaining(deadline))

        host_message = b"eventgate-host-probe"
        internal_message = b"eventgate-internal-probe"
        delivery: list[tuple[KafkaError | None, Message]] = []

        def delivered(error: KafkaError | None, message: Message) -> None:
            delivery.append((error, message))

        producer = Producer(
            {
                **conf,
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": max(1, int(remaining(deadline) * 1000)),
            }
        )
        producer.produce(handle.health_topic, value=host_message, on_delivery=delivered)
        if producer.flush(remaining(deadline)) or not delivery or delivery[0][0] is not None:
            raise EnvironmentError("Kafka did not acknowledge the host readiness probe")

        internal_args = ("exec", "-T", "kafka")
        observed = self._compose(
            handle,
            *internal_args,
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            handle.health_topic,
            "--partition",
            "0",
            "--offset",
            "0",
            "--max-messages",
            "1",
            deadline=deadline,
        )
        if observed.stdout.strip().encode() != host_message:
            raise EnvironmentError(
                "The internal Kafka listener did not read the host readiness probe"
            )

        self._compose(
            handle,
            *internal_args,
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            handle.health_topic,
            "--sync",
            "--command-property",
            "acks=all",
            input_text=internal_message.decode() + "\n",
            deadline=deadline,
        )
        observer = Consumer(
            {
                **conf,
                "group.id": f"{handle.project}-probe",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "allow.auto.create.topics": False,
            }
        )
        try:
            observer.assign([TopicPartition(handle.health_topic, 0, 1)])
            message = observer.poll(remaining(deadline))
            if message is None or message.error() or message.value() != internal_message:
                raise EnvironmentError(
                    "The host Kafka listener did not read the internal readiness probe"
                )
            for topic in topics[:2]:
                if observer.get_watermark_offsets(
                    TopicPartition(topic, 0), timeout=remaining(deadline)
                ) != (0, 0):
                    raise EnvironmentError(f"Business topic is not empty after readiness: {topic}")
        finally:
            observer.close()

    def start_application(self, handle: EnvironmentHandle, *, image_id: str | None = None) -> None:
        if handle.run_id != self.run_id or not handle.attempted:
            raise EnvironmentError("Start the environment infrastructure before its application")
        if image_id is not None:
            handle.app_image = image_id
        deadline = time.monotonic() + handle.config.timeouts.startup_seconds
        initialized = self._compose(
            handle,
            "run",
            "--rm",
            "--no-deps",
            "--pull",
            "never",
            "-T",
            "init-db",
            deadline=deadline,
        )
        if initialized.stdout or initialized.stderr:
            handle.startup_diagnostics.extend((initialized.stdout, initialized.stderr))
        asyncio.run(self._verify_database(handle, deadline))
        self._compose(
            handle,
            "up",
            "-d",
            "--no-deps",
            "--pull",
            "never",
            "consumer",
            "publisher",
            deadline=deadline,
        )
        from eventgate.adapters.payment_v1 import PaymentV1Adapter

        adapter = PaymentV1Adapter(self)
        for role in ("consumer", "publisher"):
            self._resolve_control_port(handle, role, deadline)
            adapter.wait_ready(handle, role, deadline)

    def _resolve_control_port(self, handle: EnvironmentHandle, role: Role, deadline: float) -> None:
        result = self._compose(handle, "port", role, "8080", deadline=deadline)
        found = re.fullmatch(r"127\.0\.0\.1:(\d+)", result.stdout.strip())
        if found is None:
            raise EnvironmentError(f"Expected a loopback control binding for {role}")
        handle.control_ports[role] = int(found[1])

    def container_id(self, handle: EnvironmentHandle, role: Role, deadline: float) -> str:
        result = self._compose(handle, "ps", "--all", "--quiet", role, deadline=deadline)
        container = result.stdout.strip()
        if not re.fullmatch(r"[a-f0-9]{64}", container):
            raise EnvironmentError(f"Expected one container for {role}")
        return container

    def role_state(
        self, handle: EnvironmentHandle, role: Role, deadline: float
    ) -> "ContainerState":
        container = self.container_id(handle, role, deadline)
        result = self._docker(
            "inspect", "--format", "{{json .State}}", container, deadline=deadline
        )
        try:
            state = json.loads(result.stdout)
            running, code = state["Running"], state["ExitCode"]
            if type(running) is not bool or type(code) is not int:
                raise ValueError("Invalid Docker process state")
        except (ValueError, TypeError, KeyError):
            raise EnvironmentError(f"Could not decode the container state for {role}") from None
        return ContainerState(running, code)

    def restart_role(self, handle: EnvironmentHandle, role: Role, deadline: float) -> None:
        if self.role_state(handle, role, deadline).running:
            raise EnvironmentError(f"Stop {role} before starting its existing container")
        self._compose(handle, "start", role, deadline=deadline)
        self._resolve_control_port(handle, role, deadline)

    def stop_role(self, handle: EnvironmentHandle, role: Role, deadline: float) -> "ContainerState":
        grace = max(0, int(remaining(deadline) - 0.5))
        self._compose(handle, "stop", "--timeout", str(grace), role, deadline=deadline)
        return self.role_state(handle, role, deadline)

    @staticmethod
    async def _verify_database(handle: EnvironmentHandle, deadline: float) -> None:
        async with asyncio.timeout(remaining(deadline)):
            async with await psycopg.AsyncConnection.connect(handle.postgres_dsn) as conn:
                await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor = await conn.execute(
                    "SELECT table_name, column_name, data_type, is_nullable, identity_generation "
                    "FROM information_schema.columns WHERE table_schema = 'public'"
                )
                columns = {
                    (t, c): (kind, nullable, identity)
                    for t, c, kind, nullable, identity in await cursor.fetchall()
                }
                expected = {
                    ("processed_events", "event_id"): ("text", "NO", None),
                    ("payments", "payment_id"): ("bigint", "NO", "ALWAYS"),
                    ("payments", "event_id"): ("text", "NO", None),
                    ("payments", "order_id"): ("text", "NO", None),
                    ("payments", "amount_minor"): ("bigint", "NO", None),
                    ("payments", "currency"): ("text", "NO", None),
                    ("outbox", "completion_id"): ("text", "NO", None),
                    ("outbox", "source_event_id"): ("text", "NO", None),
                    ("outbox", "payload"): ("jsonb", "NO", None),
                    ("outbox", "published_at"): ("timestamp with time zone", "YES", None),
                }
                for key, specification in expected.items():
                    if columns.get(key) != specification:
                        raise EnvironmentError(f"Incompatible database column: {'.'.join(key)}")

                cursor = await conn.execute(
                    "SELECT t.relname, pg_get_constraintdef(c.oid) FROM pg_constraint c "
                    "JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace "
                    "WHERE n.nspname = 'public'"
                )

                def normalize(definition: str) -> str:
                    return re.sub(r"[()\s]|::text|public\.", "", definition)

                constraints = {
                    (table, normalize(definition)) for table, definition in await cursor.fetchall()
                }
                required = {
                    ("processed_events", "PRIMARY KEY (event_id)"),
                    ("payments", "PRIMARY KEY (payment_id)"),
                    ("payments", "FOREIGN KEY (event_id) REFERENCES processed_events(event_id)"),
                    ("payments", "CHECK (amount_minor > 0)"),
                    ("payments", "CHECK (currency = 'EUR')"),
                    ("outbox", "PRIMARY KEY (completion_id)"),
                    ("outbox", "UNIQUE (source_event_id)"),
                    (
                        "outbox",
                        "FOREIGN KEY (source_event_id) REFERENCES processed_events(event_id)",
                    ),
                }
                for table, definition in sorted(required):
                    if (table, normalize(definition)) not in constraints:
                        raise EnvironmentError(
                            f"Incompatible database constraint: {table} {definition}"
                        )

                cursor = await conn.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_attribute a "
                    "ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0] "
                    "WHERE i.indrelid='public.payments'::regclass AND i.indisunique "
                    "AND i.indnkeyatts=1 AND a.attname='event_id')"
                )
                if await cursor.fetchone() == (True,):
                    raise EnvironmentError("payments.event_id must not be unique")

                cursor = await conn.execute(
                    "SELECT NOT EXISTS (SELECT FROM public.processed_events) "
                    "AND NOT EXISTS (SELECT FROM public.payments) AND NOT EXISTS (SELECT FROM public.outbox)"
                )
                if await cursor.fetchone() != (True,):
                    raise EnvironmentError("Application database is not empty after init-db")

    def collect_logs(self, handle: EnvironmentHandle) -> str:
        if not handle.attempted:
            return "\n".join(handle.startup_diagnostics)
        deadline = time.monotonic() + handle.config.timeouts.control_seconds
        output = self._compose(handle, "logs", "--no-color", "--timestamps", deadline=deadline)
        return "\n".join([*handle.startup_diagnostics, output.stdout, output.stderr]).strip()

    def _inventory(
        self, handle: EnvironmentHandle, kind: str, deadline: float
    ) -> list[tuple[str, bool]]:
        field_name = ".Names" if kind == "container" else ".Name"
        # Limit removal to the generated project and check this run's independent label.
        template = "{{" + field_name + '}}\t{{.Label "eventgate.run_id"}}'
        result = self._docker(
            kind,
            "ls",
            *(("-a",) if kind == "container" else ()),
            "--filter",
            f"label=com.docker.compose.project={handle.project}",
            "--filter",
            f"label=eventgate.environment_id={handle.environment_id}",
            "--format",
            template,
            deadline=deadline,
        )
        entries: list[tuple[str, bool]] = []
        prefix = handle.project + ("-" if kind == "container" else "_")
        for line in result.stdout.splitlines():
            name, _, owner = line.partition("\t")
            entries.append((name, owner == handle.run_id and name.startswith(prefix)))
        return entries

    def owned_resources(self, handle: EnvironmentHandle) -> dict[str, list[str]]:
        deadline = time.monotonic() + handle.config.timeouts.control_seconds
        return {
            kind: [name for name, owned in self._inventory(handle, kind, deadline) if owned]
            for kind in _RESOURCE_KINDS
        }

    def destroy(self, handle: EnvironmentHandle) -> None:
        if handle.run_id != self.run_id:
            raise EnvironmentError("Cannot remove another run's environment")
        if not handle.attempted:
            return
        deadline = time.monotonic() + handle.config.timeouts.cleanup_seconds
        errors: list[str] = []
        unresolved: list[str] = []
        for kind in _RESOURCE_KINDS:
            try:
                entries = self._inventory(handle, kind, deadline)
            except EnvironmentError as error:
                unresolved.append(f"{kind}:unknown ({handle.project})")
                errors.append(str(error))
                continue

            for name, owned in entries:
                if not owned:
                    errors.append(f"Refusing to remove {kind}:{name} without verified ownership")
                    unresolved.append(f"{kind}:{name}")
                    continue
                try:
                    self._docker(
                        kind,
                        "rm",
                        *(("--force",) if kind == "container" else ()),
                        name,
                        deadline=deadline,
                    )
                except EnvironmentError as error:
                    errors.append(str(error))
                    unresolved.append(f"{kind}:{name}")

        if errors:
            raise CleanupError(tuple(unresolved), tuple(errors))
        handle.attempted = False
