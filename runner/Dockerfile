# Orchestrator and report generator image. Build context is the repo root:
#   docker build -t kbench-runner:latest -f runner/Dockerfile .
FROM python:3.12-slim-bookworm

# The CLI is newer than the remote daemon (25.0.16); API version negotiation handles that.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends bash curl procps ca-certificates; \
    install -m 0755 -d /etc/apt/keyrings; \
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc; \
    chmod a+r /etc/apt/keyrings/docker.asc; \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
      > /etc/apt/sources.list.d/docker.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin; \
    rm -rf /var/lib/apt/lists/*; \
    docker --version; docker compose version

RUN pip install --no-cache-dir matplotlib==3.10.3

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    KBENCH_RESULTS_ROOT=/results

WORKDIR /app
COPY CONTRACT.md /app/CONTRACT.md
COPY infra/ /app/infra/
COPY bench/ /app/bench/
RUN mkdir -p /results

CMD ["python3", "bench/run.py", "--help"]
