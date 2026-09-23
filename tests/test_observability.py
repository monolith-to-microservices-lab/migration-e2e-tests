"""E2E: after a real CREATE, confirm it's visible through the observability
stack - Prometheus counter increments, Loki has the log line, Tempo has a
trace for the parts where tracing genuinely applies. Does NOT require (or
fabricate) a single trace spanning the WAL - see observability-infrastructure
README for why that's not possible.
"""

from __future__ import annotations

import time

import httpx
import pytest

from tests.helpers import (
    LOKI_URL,
    MONOLITH_URL,
    PROMETHEUS_URL,
    TEMPO_URL,
    USER_DSN,
    poll_for_row,
    poll_until,
)


def _prom_query(expr: str) -> list[dict]:
    r = httpx.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": expr}, timeout=5)
    r.raise_for_status()
    return r.json()["data"]["result"]


def _prom_scalar(expr: str) -> float:
    result = _prom_query(expr)
    if not result:
        return 0.0
    return float(result[0]["value"][1])


@pytest.mark.e2e
def test_prometheus_processed_counter_increases_after_real_create(run_id):
    before = _prom_scalar('sum(cdc_events_processed_total{operation="c"})')

    resp = httpx.post(f"{MONOLITH_URL}/users", json={"name": f"{run_id}_obs_prom"}, timeout=10)
    assert resp.status_code == 201
    user_id = resp.json()["id"]
    poll_for_row(USER_DSN, "SELECT id FROM users WHERE id = %s", (user_id,))

    def _increased() -> bool:
        return _prom_scalar('sum(cdc_events_processed_total{operation="c"})') > before

    poll_until(
        _increased,
        timeout=20,
        interval=1.0,
        desc="cdc_events_processed_total{operation=c} to increase",
    )


@pytest.mark.e2e
def test_loki_has_log_lines_for_the_created_entity(run_id):
    unique_name = f"{run_id}_obs_loki"
    resp = httpx.post(f"{MONOLITH_URL}/users", json={"name": unique_name}, timeout=10)
    assert resp.status_code == 201
    user_id = resp.json()["id"]
    poll_for_row(USER_DSN, "SELECT id FROM users WHERE id = %s", (user_id,))

    def _found_in_loki() -> bool:
        params = {
            "query": f'{{compose_project=~"user-service|monolito-microservice"}} |= "{user_id}"',
            "limit": "20",
            "start": str(int((time.time() - 120) * 1e9)),
            "end": str(int(time.time() * 1e9)),
        }
        r = httpx.get(f"{LOKI_URL}/loki/api/v1/query_range", params=params, timeout=5)
        r.raise_for_status()
        streams = r.json()["data"]["result"]
        return any(streams for streams in streams)

    poll_until(_found_in_loki, timeout=20, interval=1.0, desc=f"Loki log mentioning user {user_id}")


@pytest.mark.e2e
def test_tempo_has_a_trace_for_the_monolith_request():
    """Tempo has real traces for the parts where tracing genuinely applies
    (the monolith's own HTTP request -> Postgres transaction). This does NOT
    assert a single trace continues through Kafka to the consumer - that
    continuity does not exist (Debezium reads the WAL, not app context).
    """
    resp = httpx.get(f"{MONOLITH_URL}/health", timeout=10)
    assert resp.status_code == 200

    def _has_trace() -> bool:
        r = httpx.get(
            f"{TEMPO_URL}/api/search",
            params={"tags": "service.name=monolith-backend", "limit": "1"},
            timeout=5,
        )
        r.raise_for_status()
        return len(r.json().get("traces", [])) > 0

    poll_until(_has_trace, timeout=20, interval=1.0, desc="a Tempo trace for monolith-backend")
