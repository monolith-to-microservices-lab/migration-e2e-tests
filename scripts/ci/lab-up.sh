#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Bring the whole lab up from sibling checkouts, in the documented order
# (cdc-infrastructure README, "How to run it from zero"):
#
#   LAB_ROOT/
#     monolito-microservice/  cdc-infrastructure/  user-service/
#     sales-service/          api-gateway/  observability-infrastructure/ (optional)
#
#   1. shared network          5. register-connector.sh + wait RUNNING
#   2. monolith: postgres +    6. user-service + sales-service (API + CDC)
#      backend (Alembic)       7. api-gateway (Kong, default: all -> monolith)
#   3. Kafka + Kafka Connect      + wait until Kong sees every backend healthy
#   4. enable-cdc.sh           8. observability stack (WITH_OBSERVABILITY)
#
# Meant for CI runners and fresh machines. It uses the real compose files,
# project names and container names the E2E suite expects. Do NOT run it on
# a machine whose lab holds data you care about: it builds and (re)starts the
# same compose projects.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_ROOT="${LAB_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
WITH_OBSERVABILITY="${WITH_OBSERVABILITY:-false}"
CONNECT_URL="http://localhost:8083"
CONNECTOR="legacy-cdc-connector"

step() { echo; echo "=== $* ==="; }

step "network migration-network"
docker network inspect migration-network >/dev/null 2>&1 || docker network create migration-network

step "monolith (legacy postgres + backend)"
(cd "${LAB_ROOT}/monolito-microservice" && docker compose up -d --build --wait postgres backend)

step "Kafka + Kafka Connect"
cd "${LAB_ROOT}/cdc-infrastructure"
[[ -f .env ]] || cp .env.example .env
docker compose up -d --wait

step "CDC objects on the legacy database"
./postgres/enable-cdc.sh

step "register the Debezium connector"
./debezium/register-connector.sh

step "wait for connector + task RUNNING"
state=""
for _ in $(seq 1 60); do
  state="$(curl -s "${CONNECT_URL}/connectors/${CONNECTOR}/status" \
    | jq -r '"\(.connector.state)/\(.tasks[0].state // "NONE")"' 2>/dev/null)"
  [[ "${state}" == "RUNNING/RUNNING" ]] && break
  sleep 2
done
echo "connector: ${state}"
[[ "${state}" == "RUNNING/RUNNING" ]]

step "user-service (API + CDC consumer)"
(cd "${LAB_ROOT}/user-service" && docker compose up -d --build --wait)

step "sales-service (API + CDC consumer)"
(cd "${LAB_ROOT}/sales-service" && docker compose up -d --build --wait)

step "api-gateway (Kong - the clients' entry point, default routing = monolith)"
(cd "${LAB_ROOT}/api-gateway" && docker compose up -d --build --wait)
for upstream in monolith user-service sales-service; do
  health=""
  for _ in $(seq 1 60); do
    health="$(curl -s "http://127.0.0.1:8089/upstreams/${upstream}.upstream/health" | jq -r '.data[0].health' 2>/dev/null)"
    [[ "${health}" == "HEALTHY" ]] && break
    sleep 2
  done
  echo "kong -> ${upstream}: ${health}"
  [[ "${health}" == "HEALTHY" ]]
done

if [[ "${WITH_OBSERVABILITY}" == "true" ]]; then
  step "observability stack"
  (cd "${LAB_ROOT}/observability-infrastructure" && docker compose up -d --build --wait)
fi

step "lab is up"
docker ps --format 'table {{.Names}}\t{{.Status}}'
