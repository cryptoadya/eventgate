# EventGate

EventGate checks whether a new event-driven application version preserves its business
result after duplicate delivery, restarts, and controlled crashes. It runs a Baseline and
a Candidate in independent Kafka and PostgreSQL environments and reports what each did.

The MVP supports the [payment-v1 contract](docs/adapter-payment-v1.md). The demo uses
synthetic payment records. EventGate verifies one payment per input ID in its selected
scenarios. Kafka can deliver more than one physical copy of the same completion.

## Quick start

Use Python 3.12, Docker Engine, the Docker Compose CLI plugin, and Docker Buildx.
On Linux, install and start Docker Engine with the Compose and Buildx plugins. On macOS,
start Docker Desktop or use the Colima setup below. Check the connection and plugins
in the terminal where you will run EventGate.

```bash
docker version
docker compose version
docker buildx version
```

Run the following commands from the repository root. The image and dependency references
are pinned. Initial downloads and builds are separate from verification.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.lock
.venv/bin/python -m pip install --no-deps -e .
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
.venv/bin/eventgate doctor --config examples/stable-vs-stable.yaml
.venv/bin/eventgate verify --config examples/stable-vs-stable.yaml
.venv/bin/eventgate verify --config examples/stable-vs-buggy.yaml
```

The first verify should return `0` (PASS). The second should return `1` (FAIL)
for duplicate delivery and the consumer crash: the intentionally faulty version
creates a second payment. A diagnostic or infrastructure problem returns `2` (ERROR).
An interrupted run attempts to save partial evidence and remove owned resources before
returning `130`. Build the images again after changing the application source.

`verify` does not pull or build images. It resolves local image IDs before starting
and records them in the report. A release run requires all five scenarios. Set
`mode: development` to run a subset; even a complete development PASS does not
qualify as a release gate.

### Docker on macOS with Colima

For a new Homebrew setup, install the tools and link both plugins into Docker's
standard user plugin directory. If a plugin already exists there, keep it and
check its version. Start Colima with the Docker runtime if it is stopped.

```bash
brew install colima docker docker-compose docker-buildx
mkdir -p "$HOME/.docker/cli-plugins"
ln -s "$(brew --prefix docker-compose)/bin/docker-compose" "$HOME/.docker/cli-plugins/docker-compose"
ln -s "$(brew --prefix docker-buildx)/bin/docker-buildx" "$HOME/.docker/cli-plugins/docker-buildx"
colima start --runtime docker --mount none --activate=false
docker context use colima
```

`docker context use colima` saves the default for future shells. To select Colima only
for the current shell, use `export DOCKER_CONTEXT=colima` in place of that command.
Use the standard Docker configuration (`~/.docker`). An existing `DOCKER_CONFIG`,
`DOCKER_HOST`, or `DOCKER_CONTEXT` override may select a different configuration
or daemon. See the [Colima installation guide](https://colima.run/docs/installation/)
and [Docker context reference](https://docs.docker.com/reference/cli/docker/context/use/).

## Read the result

The CLI prints the unique result directory under `results/`. Open `report.html` for the
comparison, business counts, and timeline. The authoritative decision is `report.json`.
`junit.xml` uses failures for application defects and errors for untrustworthy runs.
`diagnostics.zip` preserves available evidence on FAIL or ERROR. An interrupted or
partial run identifies checks it could not evaluate.
If writing a report format fails, EventGate publishes an ERROR verdict and tries to
regenerate the other formats from it. A format that still cannot be written is omitted
from the final artifact list.

```bash
.venv/bin/eventgate scenarios list
.venv/bin/eventgate report --input results/<run-id>/
```

The second command renders new HTML and JUnit files under `rendered/` from a saved
report. It does not require Docker and does not replace the saved verdict. Its exit
status tells you whether rendering worked.

See the [architecture](docs/architecture.md), [demo walkthrough](docs/demo.md), and
[CI and required check setup](docs/ci.md). A workflow reports the gate result; an
administrator must separately require its status check to block merges.

## Local checks

The first four commands do not need Docker. The full test suite uses the same
infrastructure image references as the CLI and builds application images from the
current source tree. Run the pull commands above first.

```bash
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m mypy src
.venv/bin/python -m pytest tests/unit -q
.venv/bin/python -m pytest -q
```

The target platforms are macOS Apple Silicon and Linux x86-64. Development and Docker
runs have been exercised on macOS ARM64; a native Linux x86-64 run is still unverified.
For native Linux acceptance, verify that `uname -sm` reports `Linux x86_64` and that
the Docker server uses `linux/amd64`. Then run the Quick start and Local checks on
that host. See the architecture notes for limits of the checked behavior.

## Updating dependency locks

Use Python 3.12. The runtime lock constrains shared packages in the development lock.
The development lock also pins the wheel build backend so package tests can build
without fetching it.

```bash
python3 -m venv .lock-venv
.lock-venv/bin/python -m pip install pip==25.1.1 pip-tools==7.5.0
.lock-venv/bin/python -m piptools compile --strip-extras --output-file requirements-runtime.lock pyproject.toml
.lock-venv/bin/python -m piptools compile --strip-extras --allow-unsafe --extra dev --constraint requirements-runtime.lock --output-file requirements-dev.lock pyproject.toml
```

Add `--upgrade` to both compile commands to refresh all pins. Run them in that order.
