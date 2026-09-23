#!/usr/bin/env bash
# Collect container logs + CDC state into $1 (default: lab-logs/) for a CI
# artifact. Read-only: it never stops or changes anything. Every command is
# allowed to fail individually so one missing project does not hide the rest.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_ROOT="${LAB_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
mkdir -p "${1:-lab-logs}"
OUT="$(cd "${1:-lab-logs}" && pwd)"

docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' > "${OUT}/containers.txt"

for repo in monolito-microservice cdc-infrastructure user-service sales-service observability-infrastructure; do
  if [[ -d "${LAB_ROOT}/${repo}" ]]; then
    (cd "${LAB_ROOT}/${repo}" && docker compose logs --no-color --timestamps > "${OUT}/${repo}.log" 2>&1)
  fi
done

curl -s http://localhost:8083/connectors/legacy-cdc-connector/status > "${OUT}/connector-status.json"
docker exec monolito-microservice-postgres-1 psql -U postgres -d monolith -c \
  "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;" \
  > "${OUT}/replication-slots.txt" 2>&1

echo "logs collected in ${OUT}/"
