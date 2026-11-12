# syntax=docker/dockerfile:1.7
FROM eclipse-temurin:21-jre-noble@sha256:db724162e4ff5f71fa0a2815026314d9e5e0e9a139f49be1d4717e6930e93341 AS java
FROM python:3.11.18-slim-bookworm@sha256:061c190f3d5815b341a18dfe8a45771f8ba68c15a607ea56997fde3009ac4eec

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    SPARK_LOCAL_DIRS=/tmp/labgdd-spark

WORKDIR /opt/labgdd
COPY requirements-batch.lock pyproject.toml README.md ./
RUN python -m pip install --require-hashes -r requirements-batch.lock
COPY src ./src
RUN python -m pip install --no-build-isolation --no-deps . \
    && python -m pip check \
    && python -m pip uninstall --yes setuptools wheel

COPY config ./config
COPY scripts/smoke_silver_container.py ./scripts/smoke_silver_container.py
COPY scripts/smoke_gold_container.py ./scripts/smoke_gold_container.py
RUN groupadd --gid 10001 labgdd \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin labgdd \
    && mkdir -p /opt/labgdd/data/silver /opt/labgdd/data/gold /tmp/labgdd-spark \
    && chown -R 10001:10001 /opt/labgdd/data/silver /opt/labgdd/data/gold /tmp/labgdd-spark

USER 10001:10001
ENTRYPOINT ["labgdd-silver"]
CMD ["--config", "config/project.toml", "--request", "config/silver.toml", "--master", "local[2]"]
