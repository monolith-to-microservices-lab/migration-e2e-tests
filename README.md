# migration-e2e-tests

End-to-end tests for the Strangler Fig migration lab. Lives in its own repo
on purpose: it tests the **system** (monolith + Postgres + Debezium + Kafka +
both consumers + both destination DBs), not any single service, so it
doesn't belong inside `user-service` or `sales-service`.

No mocks anywhere in this repo. Every test hits the real running lab.

## Prerequisites

All lab repos already running (`docker compose up -d` in each):
`monolito-microservice`, `user-service`, `sales-service`, `cdc-infrastructure`,
`api-gateway`, `observability-infrastructure`.

## Entry point: the API Gateway

The suite acts as a **client**, so it goes through the gateway
(`api-gateway`, Kong on `http://localhost:8088`), exactly like the frontend:

```text
TEST -> KONG -> MONOLITH / MICROSERVICE -> DB / CDC
```

- Every client write (`POST /users`, `POST /sales`) goes through Kong, and
  `api_post()` **fails the test if it was not served by the monolith**. While
  the monolith is the source of truth, a write elsewhere would silently break
  every CDC assertion.
- `MONOLITH_URL` is only used for checks *of* the monolith itself (its
  `/health` and traces).
- `E2E_API_URL=http://localhost:8000` runs the suite without a gateway.
- `tests/test_gateway.py` drives route switches with the **operator scripts**
  of the sibling `api-gateway` checkout (`pwsh` on CI, Windows PowerShell
  locally; override with `E2E_GATEWAY_REPO`). It expects the gateway on
  `mode-1-all-monolith` and always returns it there.

| Scenario | Marker | What is proven |
|---|---|---|
| E2E 1: default | `smoke gateway` | every route → monolith; CREATE USER + CREATE SALE through Kong reach both destination DBs via CDC; the client's `X-Request-ID` is the one the monolith logged |
| E2E 2: read switch | `smoke gateway` | `GET /users/{id}` moves to user-service with the **same URL** and an **identical body**; 404 contract kept; writes stay on the monolith |
| E2E 3: rollback | `smoke gateway` | back to the monolith, gateway healthy, same URL, the monolith's log has the request id |
| E2E 4: upstream down | `gateway failure` | user-service stopped → controlled 502/503/504 (no fake success, no fallback), counted in `kong_http_requests_total` and Prometheus, logged with `upstream`; recovery after restart (~30-60 s: a stopped container leaves Docker DNS and Kong's balancer re-queries a failed name every 30 s) |

The gateway table in `test-results/<run_id>.md` records REQUEST / METHOD /
PATH / GATEWAY / DESTINATION / STATUS / LATENCY / REQUEST_ID for each step.

## Setup

```bash
python -m venv .venv
.venv\Scripts\pip install -e .
```

## Running

```bash
pytest -m "e2e and not failure"   # happy path + observability - safe, non-destructive
pytest -m "e2e and failure"       # chaos: stops/restarts real lab containers - see below
pytest                            # everything
```

Every run writes evidence to `test-results/<run_id>.json` and `.md` - a
timestamped record of every operation's Kafka topic/partition/offset and
end-to-end latency.

## What `-m failure` actually does

`tests/test_resilience.py` uses `docker stop`/`docker start` (**never**
`down`, **never** `-v`) against real lab containers: `user-postgres`,
`user-service-cdc`, `cdc-kafka`, `cdc-connect`. Each scenario is wrapped in a
`try/finally` that restarts the container even if the assertions in between
fail - but for ~10-100 seconds per test, that piece of the lab is genuinely
down. Don't run this against a lab someone else is actively using.

Observed, real (not assumed) behavior worth knowing before you run it:
- Restarting a container right after `docker start` can hit a one-time DNS
  resolution race in Docker's embedded resolver (the container can't yet
  resolve a sibling's hostname), causing one crash + auto-restart via
  `restart: unless-stopped` before it stabilizes. Timeouts in these tests are
  generous on purpose to accommodate this.
- Stopping Kafka: Kafka Connect's worker coordinator detects the broker is
  gone and stops the connector task cleanly (rebalance-safe); the
  confluent-kafka consumer clients log connection-refused retries and
  reconnect on their own once the broker is back - no manual intervention
  needed for either.
- Stopping Kafka Connect: WAL lag on `legacy_cdc_slot` grows for as long as
  Connect is down and shrinks back down once it's restarted - nothing is
  lost, the connector resumes from the slot's LSN.

## Known limitation: test data accumulation

Happy-path and observability tests create real rows via the real monolith
API, each named with the run's `test_run_id` prefix (e.g.
`e2e_20260923_002801_user_create`) so they're always identifiable. Most
scenarios clean up after themselves (the DELETE step in a CREATE/UPDATE/
DELETE flow *is* the cleanup); `test_combined.py` deliberately does not, since
its point is just to prove a linked user+sale both propagate. This is a lab
environment, not production - accumulated test rows are harmless and
identifiable by prefix if you ever want to prune them. Never run a mass
`DELETE`/`TRUNCATE` against the legacy or destination databases to "clean
up" - a targeted `DELETE ... WHERE id = <id>` (as these tests already do) is
the only pattern used here.

## Markers

`e2e` (everything here), `smoke`, `gateway` (traffic through Kong incl.
route switch/rollback), `slow`, `failure` (implies real container
stop/start - see above).

## CI

| Workflow | Trigger | Job | What runs |
|---|---|---|---|
| `ci.yml` | every PR / push to `main` | **Lint**, **Type Check** | ruff, mypy, ShellCheck (`scripts/ci`) |
| | | **E2E Smoke** | checks out the lab repos (`main`), brings the **real** lab up with `scripts/ci/lab-up.sh` (monolith + legacy Postgres + Kafka + Debezium + both services + both consumers + destination DBs + **api-gateway**) and runs `pytest -m smoke` through the gateway (users, sales, linked user+sale, gateway default routing, read switch + rollback) |
| `e2e-full.yml` | manual (nightly later) | **E2E Full + Chaos** | same lab **plus** the observability stack; `-m "e2e and not failure"` then `-m failure` (real `docker stop/start`) |
| `security.yml` | every PR / push, weekly | **Security** | Gitleaks, pip-audit |

On failure, container logs, connector status and replication-slot state are
uploaded as an artifact (`scripts/ci/collect-logs.sh`), together with the
evidence in `test-results/`.

`scripts/ci/lab-up.sh` is for CI runners and fresh machines: it builds and starts
the same compose projects as your local lab, so do not run it where the lab holds
data you care about.
