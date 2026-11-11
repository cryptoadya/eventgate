FROM python:3.12-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df

ARG PAYMENT_VARIANT=stable
ARG SOURCE_REVISION=unknown
ARG SOURCE_STATE=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      io.eventgate.source-state="${SOURCE_STATE}" \
      io.eventgate.payment-contract="payment-v1" \
      io.eventgate.payment-variant="${PAYMENT_VARIANT}"

RUN case "$PAYMENT_VARIANT" in stable|buggy) ;; *) echo "Unknown PAYMENT_VARIANT" >&2; exit 1 ;; esac
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PAYMENT_VARIANT="${PAYMENT_VARIANT}"
WORKDIR /app
COPY pyproject.toml README.md LICENSE requirements-runtime.lock ./
RUN python -m pip install --no-cache-dir -r requirements-runtime.lock
COPY src ./src
RUN python -m pip install --no-cache-dir --no-deps . \
    && install -d -o 10001 -g 10001 /var/lib/eventgate-control
USER 10001:10001
ENTRYPOINT ["python", "-m", "eventgate_demo.cli"]
