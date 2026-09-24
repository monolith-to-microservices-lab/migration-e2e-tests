"""Shared helpers for the E2E suite: polling (no fixed sleeps), connections
to the real lab endpoints, a run-id generator, evidence collection, and the
JSON+Markdown report writer. This suite exercises the REAL stack - no mocks,
no fakes, for any of monolith/Postgres/Debezium/Kafka/consumer/destination DB.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "test-results"

# The CLIENT entry point is the API Gateway (api-gateway repo): every test
# that acts as a client goes through it, exactly like the frontend. By
# default Kong routes everything to the monolith, so the CDC assertions are
# unchanged. MONOLITH_URL stays for checks OF the monolith itself (its own
# /health and traces). E2E_API_URL=http://localhost:8000 runs the suite
# without a gateway.
API_URL = os.environ.get("E2E_API_URL", "http://localhost:8088")
GATEWAY_ADMIN_URL = os.environ.get("E2E_GATEWAY_ADMIN_URL", "http://127.0.0.1:8089")
GATEWAY_STATUS_URL = os.environ.get("E2E_GATEWAY_STATUS_URL", "http://127.0.0.1:8090")
GATEWAY_CONTAINER = "api-gateway-api-gateway-1"
MONOLITH_URL = "http://localhost:8000"
USER_SERVICE_URL = "http://localhost:8001"
LEGACY_DSN = "postgresql://postgres:postgres@localhost:5432/monolith"
USER_DSN = "postgresql://user_service:user_service@localhost:5433/user_service"
SALES_DSN = "postgresql://sales:sales@localhost:5434/sales"
PROMETHEUS_URL = "http://localhost:9090"
LOKI_URL = "http://localhost:3100"
TEMPO_URL = "http://localhost:3200"
GATEWAY_REPO = Path(os.environ.get("E2E_GATEWAY_REPO", str(REPO_ROOT.parent / "api-gateway")))
DEFAULT_ROUTING = {
    "users-read": "monolith",
    "users-write": "monolith",
    "sales-read": "monolith",
    "sales-write": "monolith",
}


def api_post(path: str, payload: dict) -> httpx.Response:
    """POST as a client (through the gateway). While the monolith is the
    source of truth, every write MUST be served by it - fail loudly if the
    gateway routed it anywhere else (it would silently break CDC tests)."""
    resp = httpx.post(f"{API_URL}{path}", json=payload, timeout=10)
    upstream = resp.headers.get("X-Upstream-Service")
    if upstream is not None and upstream != "monolith":
        raise AssertionError(
            f"POST {path} was served by {upstream!r}, not the monolith "
            "- gateway not on its default routing?"
        )
    return resp


def gateway_routing() -> dict[str, str]:
    """Route -> upstream as Kong is routing RIGHT NOW (Admin API)."""
    services = {
        s["id"]: s["name"]
        for s in httpx.get(f"{GATEWAY_ADMIN_URL}/services", timeout=5).json()["data"]
    }
    return {
        r["name"]: services[r["service"]["id"]]
        for r in httpx.get(f"{GATEWAY_ADMIN_URL}/routes", timeout=5).json()["data"]
        if r.get("service")
    }


def gateway_script(name: str, *args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    """Run one of the api-gateway operator scripts - the same command an
    operator runs (pwsh on Linux CI, Windows PowerShell locally)."""
    exe = shutil.which("pwsh") or shutil.which("powershell")
    if exe is None:
        raise RuntimeError("PowerShell (pwsh/powershell) is required to drive the gateway scripts")
    cmd = [exe, "-NoProfile", "-NonInteractive"]
    if os.name == "nt":
        cmd += ["-ExecutionPolicy", "Bypass"]
    cmd += ["-File", str(GATEWAY_REPO / "scripts" / f"{name}.ps1"), *args]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=GATEWAY_REPO)
    if r.returncode != 0:
        raise RuntimeError(f"{name}.ps1 failed ({r.returncode}):\n{r.stdout}\n{r.stderr}")
    return r


def new_run_id() -> str:
    return f"e2e_{datetime.now(UTC):%Y%m%d_%H%M%S}"


class TimeoutExceeded(AssertionError):
    pass


def poll_until(
    predicate: Callable[[], bool],
    timeout: float = 15.0,
    interval: float = 0.2,
    desc: str = "condition",
) -> None:
    """Poll `predicate` until it returns truthy, or raise. Never a fixed
    sleep - returns as soon as the condition is met.
    """
    deadline = time.monotonic() + timeout
    last_exc = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # noqa: BLE001 - keep polling, surface last error on timeout
            last_exc = exc
        time.sleep(interval)
    suffix = f" (last error: {last_exc})" if last_exc else ""
    raise TimeoutExceeded(f"timed out after {timeout}s waiting for: {desc}{suffix}")


def poll_for_row(dsn: str, query: str, params: tuple, timeout: float = 15.0, interval: float = 0.3):
    """Poll a DSN with a query until it returns a row, then return it."""
    holder: dict[str, Any] = {}

    def _check() -> bool:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            if row is not None:
                holder["row"] = row
                return True
        return False

    poll_until(
        _check,
        timeout=timeout,
        interval=interval,
        desc=f"row for {query % params if '%s' not in query else query}",
    )
    return holder["row"]


def poll_for_absence(
    dsn: str, query: str, params: tuple, timeout: float = 15.0, interval: float = 0.3
) -> None:
    def _check() -> bool:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone() is None

    poll_until(_check, timeout=timeout, interval=interval, desc=f"absence for {query}")


def legacy_execute(sql: str, params: tuple = ()) -> None:
    """The monolith has no UPDATE/DELETE endpoints (verified, not assumed -
    see both services' READMEs) - direct SQL against the legacy DB is the
    only way to exercise those operations, and is exactly how Debezium sees
    any change regardless of how it was made.
    """
    with psycopg.connect(LEGACY_DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql, params)


def docker(*args: str, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


def docker_stop(container: str) -> None:
    r = docker("stop", container)
    if r.returncode != 0:
        raise RuntimeError(f"docker stop {container} failed: {r.stderr}")


def docker_start(container: str) -> None:
    r = docker("start", container)
    if r.returncode != 0:
        raise RuntimeError(f"docker start {container} failed: {r.stderr}")


def container_log_tail(container: str, lines: int = 200) -> str:
    r = docker("logs", "--tail", str(lines), container)
    return (r.stdout or "") + (r.stderr or "")


def container_is_running(container: str) -> bool:
    r = docker("inspect", "--format", "{{.State.Running}}", container)
    return r.stdout.strip() == "true"


def wait_container_healthy(container: str, timeout: float = 60.0) -> None:
    def _check() -> bool:
        r = docker("inspect", "--format", "{{.State.Health.Status}}", container)
        status = r.stdout.strip()
        if status:
            return status == "healthy"
        return container_is_running(container)  # no healthcheck defined - fall back

    poll_until(_check, timeout=timeout, interval=1.0, desc=f"{container} healthy")


@dataclass
class EvidenceRow:
    run_id: str
    flow: str
    operation: str
    entity: str
    entity_id: int
    topic: str = ""
    partition: int | None = None
    offset: int | None = None
    source_ts_ms: int | None = None
    consumer_applied_at: str = ""
    processing_time_s: float | None = None
    end_to_end_latency_s: float | None = None
    result: str = "OK"
    detail: str = ""


@dataclass
class GatewayEvidenceRow:
    step: str
    method: str
    path: str
    gateway: str
    destination: str
    status: int
    latency_ms: float
    request_id: str


@dataclass
class EvidenceCollector:
    run_id: str
    rows: list[EvidenceRow] = field(default_factory=list)
    gateway_rows: list[GatewayEvidenceRow] = field(default_factory=list)

    def add(self, row: EvidenceRow) -> None:
        self.rows.append(row)

    def gateway(self, step: str, resp: httpx.Response) -> GatewayEvidenceRow:
        """Record one client request through Kong: who served it and how."""
        row = GatewayEvidenceRow(
            step=step,
            method=resp.request.method,
            path=resp.request.url.path,
            gateway="Kong"
            if "X-Kong-Proxy-Latency" in resp.headers or "X-Request-ID" in resp.headers
            else "-",
            destination=resp.headers.get(
                "X-Upstream-Service", f"(gateway answered: {resp.status_code})"
            ),
            status=resp.status_code,
            latency_ms=round(resp.elapsed.total_seconds() * 1000, 1),
            request_id=resp.headers.get("X-Request-ID", ""),
        )
        self.gateway_rows.append(row)
        return row

    def from_consumer_log(
        self,
        container: str,
        flow: str,
        operation: str,
        entity: str,
        entity_id: int,
        op_code: str,
        timeout: float = 15.0,
    ) -> EvidenceRow:
        """Extract topic/partition/offset from the consumer's own
        structured JSON log line (`cdc.applied`) for this entity/op -
        the same log line already used to prove propagation in prior manual
        validation, now captured programmatically.
        """
        holder: dict[str, Any] = {}

        def _check() -> bool:
            log = container_log_tail(container, lines=500)
            for line in reversed(log.splitlines()):
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("message") != "cdc.applied":
                    continue
                key = "user_id" if entity == "user" else "sale_id"
                if payload.get(key) == entity_id and payload.get("op") == op_code:
                    holder["payload"] = payload
                    holder["timestamp"] = payload.get("timestamp")
                    return True
            return False

        poll_until(
            _check,
            timeout=timeout,
            interval=0.3,
            desc=f"consumer log for {entity} {entity_id} op={op_code}",
        )
        payload = holder["payload"]
        row = EvidenceRow(
            run_id=self.run_id,
            flow=flow,
            operation=operation,
            entity=entity,
            entity_id=entity_id,
            topic=payload.get("topic", ""),
            partition=payload.get("partition"),
            offset=payload.get("offset"),
            consumer_applied_at=holder["timestamp"],
            result="OK",
        )
        self.add(row)
        return row

    def write(self) -> tuple[Path, Path]:
        RESULTS_DIR.mkdir(exist_ok=True)
        json_path = RESULTS_DIR / f"{self.run_id}.json"
        md_path = RESULTS_DIR / f"{self.run_id}.md"

        json_path.write_text(
            json.dumps(
                {
                    "run_id": self.run_id,
                    "rows": [vars(r) for r in self.rows],
                    "gateway": [vars(r) for r in self.gateway_rows],
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

        lines = [f"# E2E RUN: {self.run_id}", ""]
        by_flow: dict[str, list[EvidenceRow]] = {}
        for row in self.rows:
            by_flow.setdefault(row.flow, []).append(row)
        for flow, rows in by_flow.items():
            lines.append(f"## {flow}")
            for r in rows:
                lines.append(f"\n### {r.operation}")
                lines.append(f"- entity: {r.entity} {r.entity_id}")
                if r.topic:
                    lines.append(f"- Kafka topic: {r.topic}")
                    lines.append(f"- partition: {r.partition}")
                    lines.append(f"- offset: {r.offset}")
                if r.end_to_end_latency_s is not None:
                    lines.append(f"- end-to-end latency: {r.end_to_end_latency_s:.3f}s")
                lines.append(f"- destination: {r.result}")
                if r.detail:
                    lines.append(f"- detail: {r.detail}")
            lines.append("")
        if self.gateway_rows:
            lines += [
                "## gateway (client -> Kong -> destination)",
                "",
                "| STEP | METHOD | PATH | GATEWAY | DESTINATION | STATUS | LATENCY | REQUEST_ID |",
                "|---|---|---|---|---|---|---|---|",
            ]
            for g in self.gateway_rows:
                lines.append(
                    f"| {g.step} | {g.method} | {g.path} | {g.gateway} | {g.destination} "
                    f"| {g.status} | {g.latency_ms} ms | {g.request_id} |"
                )
            lines.append("")
        md_path.write_text("\n".join(lines), encoding="utf-8")
        return json_path, md_path
