"""E2E through the API Gateway (Kong, api-gateway repo) - the REAL lab:

    TEST -> KONG -> MONOLITH / MICROSERVICE -> DB / CDC

1. DEFAULT       every route -> monolith; CREATE USER + CREATE SALE through
                 Kong land in the monolith and reach both destination DBs via CDC
2. READ SWITCH   GET /users/{id} moves monolith -> user-service with the SAME
                 URL and the SAME semantics (identical body for the same row)
3. ROLLBACK      user-service -> monolith; gateway stays healthy, same URL
4. UPSTREAM DOWN reads on user-service, user-service stopped -> controlled
                 5xx, counted in Kong's metrics and logged with the upstream;
                 no fallback; recovers when it is back (-m failure)

Route switches use the operator scripts of the api-gateway repo, exactly as
a human would. Every scenario starts on, and returns to, the default routing
(mode-1-all-monolith). Writes are NEVER routed to a microservice here.
"""

from __future__ import annotations

import time
import uuid

import httpx
import pytest

from tests.helpers import (
    API_URL,
    DEFAULT_ROUTING,
    GATEWAY_CONTAINER,
    GATEWAY_STATUS_URL,
    MONOLITH_URL,
    PROMETHEUS_URL,
    SALES_DSN,
    USER_DSN,
    api_post,
    container_log_tail,
    docker_start,
    docker_stop,
    gateway_routing,
    gateway_script,
    poll_for_row,
    poll_until,
    wait_container_healthy,
)

USER_SERVICE_CONTAINER = "user-service-user-service-1"


def gw_get(path: str, **kw) -> httpx.Response:
    return httpx.get(f"{API_URL}{path}", timeout=15, **kw)


@pytest.fixture
def default_routing():
    """Start on the default routing and always come back to it."""
    assert gateway_routing() == DEFAULT_ROUTING, (
        "E2E expects the gateway on mode-1-all-monolith - run "
        "api-gateway/scripts/rollback-all-to-monolith.ps1 first"
    )
    yield
    gateway_script("rollback-all-to-monolith", "-Reason", "e2e cleanup")
    assert gateway_routing() == DEFAULT_ROUTING


def _kong_counter(route: str, service: str, codes: tuple[str, ...]) -> float:
    text = httpx.get(f"{GATEWAY_STATUS_URL}/metrics", timeout=5).text
    total = 0.0
    for line in text.splitlines():
        if line.startswith("kong_http_requests_total{") and f'route="{route}"' in line:
            if f'service="{service}"' in line and any(f'code="{c}"' in line for c in codes):
                total += float(line.rsplit(" ", 1)[1])
    return total


@pytest.mark.e2e
@pytest.mark.smoke
@pytest.mark.gateway
def test_e2e1_default_routing_create_user_and_sale_through_kong(default_routing, evidence, run_id):
    user = api_post("/users", {"name": f"{run_id}_gw_user"})
    assert user.status_code == 201, user.text
    assert user.headers["X-Upstream-Service"] == "monolith"
    evidence.gateway("E2E1 create user", user)
    user_id = user.json()["id"]

    sale = api_post("/sales", {"user_id": user_id, "item_name": f"{run_id}_gw_sale", "quantity": 2})
    assert sale.status_code == 201, sale.text
    assert sale.headers["X-Upstream-Service"] == "monolith"
    assert sale.json()["user_name"] == f"{run_id}_gw_user"  # the monolith's JOIN
    evidence.gateway("E2E1 create sale", sale)
    sale_id = sale.json()["id"]

    # the current pipeline, unchanged: monolith -> WAL -> Debezium -> Kafka -> consumers
    assert poll_for_row(USER_DSN, "SELECT id, name FROM users WHERE id = %s", (user_id,)) == (
        user_id,
        f"{run_id}_gw_user",
    )
    assert poll_for_row(SALES_DSN, "SELECT id, user_id FROM sales WHERE id = %s", (sale_id,)) == (
        sale_id,
        user_id,
    )
    # the request id the client saw is the one the monolith logged
    rid = user.headers["X-Request-ID"]
    assert rid in container_log_tail("monolito-microservice-backend-1", lines=400)


@pytest.mark.e2e
@pytest.mark.smoke
@pytest.mark.gateway
def test_e2e2_e2e3_read_switch_same_url_same_semantics_then_rollback(
    default_routing, evidence, run_id
):
    user = api_post("/users", {"name": f"{run_id}_gw_read_switch"})
    assert user.status_code == 201, user.text
    user_id = user.json()["id"]
    poll_for_row(USER_DSN, "SELECT id FROM users WHERE id = %s", (user_id,))  # replica has it
    url = f"/users/{user_id}"

    before = gw_get(url)
    assert before.status_code == 200 and before.headers["X-Upstream-Service"] == "monolith"
    evidence.gateway("E2E2 before switch", before)

    # --- E2E 2: READ SWITCH -------------------------------------------------
    gateway_script("route-users-to-service", "-Reason", f"e2e {run_id} read switch")
    assert gateway_routing()["users-read"] == "user-service"
    assert gateway_routing()["users-write"] == "monolith"  # writes did not move

    after = gw_get(url)  # the SAME URL - the client changed nothing
    assert after.status_code == 200
    assert after.headers["X-Upstream-Service"] == "user-service"
    assert after.json() == before.json()  # same semantics: identical representation
    evidence.gateway("E2E2 after switch (same URL)", after)

    missing = gw_get("/users/999999999")
    assert missing.status_code == 404  # same status contract on the error path
    assert missing.headers["X-Upstream-Service"] == "user-service"

    listing = gw_get("/users")
    assert listing.status_code == 200 and listing.headers["X-Upstream-Service"] == "user-service"
    assert any(u["id"] == user_id for u in listing.json())

    write = api_post("/users", {"name": f"{run_id}_gw_write_during_read_switch"})
    assert write.status_code == 201  # api_post asserts it went to the monolith
    evidence.gateway("E2E2 write during read switch", write)

    # --- E2E 3: ROLLBACK -------------------------------------------------------
    gateway_script("route-users-to-monolith", "-Reason", f"e2e {run_id} rollback")
    assert gateway_routing() == DEFAULT_ROUTING
    assert httpx.get(f"{API_URL}/gateway/health", timeout=5).status_code == 200
    assert httpx.get(f"{GATEWAY_STATUS_URL}/status/ready", timeout=5).status_code == 200

    back = gw_get(url)
    assert back.status_code == 200
    assert back.headers["X-Upstream-Service"] == "monolith"
    assert back.json() == before.json()
    evidence.gateway("E2E3 after rollback (same URL)", back)
    # and the monolith really served it (its own log has this request id)
    assert back.headers["X-Request-ID"] in container_log_tail(
        "monolito-microservice-backend-1", lines=400
    )


@pytest.mark.e2e
@pytest.mark.gateway
@pytest.mark.failure
@pytest.mark.slow
def test_e2e4_user_service_down_controlled_error_then_recovery(default_routing, evidence, run_id):
    gateway_script("route-users-to-service", "-Reason", f"e2e {run_id} upstream-down drill")
    ok = gw_get("/users/1")
    assert ok.headers.get("X-Upstream-Service") == "user-service"
    failures_before = _kong_counter("users-read", "user-service", ("502", "503", "504"))

    docker_stop(USER_SERVICE_CONTAINER)
    try:
        seen: list[int] = []

        def _gateway_fails() -> bool:
            r = gw_get("/users/1")
            seen.append(r.status_code)
            return r.status_code in (502, 503, 504)

        poll_until(
            _gateway_fails, timeout=40, interval=0.5, desc="gateway 5xx for user-service down"
        )
        rid = f"e2e-upstream-down-{uuid.uuid4()}"
        down = gw_get("/users/1", headers={"X-Request-ID": rid})
        assert down.status_code in (502, 503, 504), down.text
        assert "X-Upstream-Service" not in down.headers  # no backend answered - no fake success
        evidence.gateway("E2E4 user-service stopped", down)

        # NO silent fallback: reads stay on user-service, writes stay on the monolith
        assert gateway_routing()["users-read"] == "user-service"
        assert api_post("/users", {"name": f"{run_id}_gw_during_outage"}).status_code == 201

        # metric: Kong counted the failure for route users-read -> user-service
        poll_until(
            lambda: (
                _kong_counter("users-read", "user-service", ("502", "503", "504")) > failures_before
            ),
            timeout=10,
            desc="kong_http_requests_total 5xx for users-read/user-service",
        )

        # log: one JSON access line with the upstream and the status
        def _logged() -> bool:
            for line in container_log_tail(GATEWAY_CONTAINER, lines=400).splitlines():
                if rid in line and '"upstream":"user-service"' in line:
                    return f'"status":{down.status_code}' in line
            return False

        poll_until(_logged, timeout=10, desc="Kong access log for the failed request")

        # the same failure is visible in Prometheus when the observability stack runs
        try:
            q = (
                'sum(kong_http_requests_total{route="users-read",'
                'service="user-service",code=~"50[234]"})'
            )
            poll_until(
                lambda: (
                    float(
                        httpx.get(
                            f"{PROMETHEUS_URL}/api/v1/query", params={"query": q}, timeout=5
                        ).json()["data"]["result"][0]["value"][1]
                    )
                    > 0
                ),
                timeout=30,
                interval=2,
                desc="Prometheus sees the gateway 5xx",
            )
        except httpx.ConnectError:
            # observability stack not running (e.g. smoke CI): Kong's own
            # /metrics endpoint was already checked above
            pass
    finally:
        docker_start(USER_SERVICE_CONTAINER)

    wait_container_healthy(USER_SERVICE_CONTAINER, timeout=90)
    started = time.monotonic()

    # A STOPPED container disappears from Docker's DNS. Once Kong's balancer
    # saw that NXDOMAIN it re-queries only every 30 s (hard-coded in Kong's
    # balancer), then the active health check needs one success: measured
    # recovery ~30-60 s after the container is healthy again. (Where the
    # backend name stays resolvable - a Kubernetes Service, ECS service
    # discovery - recovery is the health-check interval, ~5 s.)
    def _stably_recovered() -> bool:
        # DNS re-resolution and the health check converge separately: a single
        # 200 can be followed by a transient 503, so require 3 in a row.
        return all(
            gw_get("/users/1").headers.get("X-Upstream-Service") == "user-service" for _ in range(3)
        )

    poll_until(_stably_recovered, timeout=120, interval=1, desc="stable recovery")
    recovered = gw_get("/users/1")
    assert recovered.headers["X-Upstream-Service"] == "user-service"
    evidence.gateway(f"E2E4 recovered after {time.monotonic() - started:.1f}s", recovered)
    # direct check that it is the same data the monolith has for that row
    assert recovered.json() == httpx.get(f"{MONOLITH_URL}/users/1", timeout=10).json()
