#!/usr/bin/env bash
# Start Docker services (ES + Doris) and run E2E tests.
#
# Usage:
#   ./docker/e2e_run.sh          # start services, run tests, keep services running
#   ./docker/e2e_run.sh --down   # tear down services after tests
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
ENV_FILE="$SCRIPT_DIR/.env"

TEAR_DOWN=false
if [[ "${1:-}" == "--down" ]]; then
    TEAR_DOWN=true
fi

# Use .env if it exists, otherwise fall back to .env.example
if [[ ! -f "$ENV_FILE" ]]; then
    if [[ -f "$SCRIPT_DIR/.env.example" ]]; then
        cp "$SCRIPT_DIR/.env.example" "$ENV_FILE"
        echo "Created .env from .env.example"
    fi
fi

echo "==> Starting Docker services..."
docker compose -f "$COMPOSE_FILE" up -d

echo "==> Waiting for Elasticsearch to be healthy..."
timeout=120
elapsed=0
while ! curl -sf http://localhost:9200/_cluster/health > /dev/null 2>&1; do
    if (( elapsed >= timeout )); then
        echo "ERROR: Elasticsearch did not become healthy within ${timeout}s"
        docker compose -f "$COMPOSE_FILE" logs elasticsearch
        exit 1
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    echo "    ... waiting ($elapsed/${timeout}s)"
done
echo "    Elasticsearch is ready."

echo "==> Waiting for Doris to be healthy..."
timeout=300
elapsed=0
while ! mysql -h 127.0.0.1 -P 9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | grep -q "Alive: true"; do
    if (( elapsed >= timeout )); then
        echo "ERROR: Doris did not become healthy within ${timeout}s"
        docker compose -f "$COMPOSE_FILE" logs doris
        exit 1
    fi
    sleep 10
    elapsed=$((elapsed + 10))
    echo "    ... waiting ($elapsed/${timeout}s)"
done
echo "    Doris is ready."

echo "==> Installing E2E dependencies..."
cd "$PROJECT_DIR"
pip install -e ".[e2e]" --quiet 2>/dev/null || pip install -e ".[e2e]"

echo "==> Running E2E tests..."
pytest tests/test_e2e.py -m e2e -v --tb=short
TEST_RC=$?

if $TEAR_DOWN; then
    echo "==> Tearing down Docker services..."
    docker compose -f "$COMPOSE_FILE" down -v
fi

echo "==> Done (exit code: $TEST_RC)"
exit $TEST_RC
