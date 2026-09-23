# migration-e2e-tests

End-to-end tests for the Strangler Fig migration lab. Lives in its own repo
on purpose: it tests the **system** (monolith + Postgres + Debezium + Kafka +
both consumers + both destination DBs), not any single service, so it
doesn't belong inside `user-service` or `sales-service`.

No mocks anywhere in this repo. Every test hits the real running lab.

## Prerequisites

All 5 lab repos already running (`docker compose up -d` in each):
`monolito-microservice`, `user-service`, `sales-service`, `cdc-infrastructure`,
`observability-infrastructure`.

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

`e2e` (everything here), `slow`, `failure` (implies real container
stop/start - see above).
