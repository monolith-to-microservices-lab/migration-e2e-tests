"""Shared helpers for the E2E suite: polling (no fixed sleeps), connections
to the real lab endpoints, a run-id generator, evidence collection, and the
JSON+Markdown report writer. This suite exercises the REAL stack - no mocks,
no fakes, for any of monolith/Postgres/Debezium/Kafka/consumer/destination DB.
"""

from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "test-results"

MONOLITH_URL = "http://localhost:8000"
LEGACY_DSN = "postgresql://postgres:postgres@localhost:5432/monolith"
USER_DSN = "postgresql://user_service:user_service@localhost:5433/user_service"
SALES_DSN = "postgresql://sales:sales@localhost:5434/sales"
PROMETHEUS_URL = "http://localhost:9090"
LOKI_URL = "http://localhost:3100"
TEMPO_URL = "http://localhost:3200"


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
class EvidenceCollector:
    run_id: str
    rows: list[EvidenceRow] = field(default_factory=list)

    def add(self, row: EvidenceRow) -> None:
        self.rows.append(row)

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
                {"run_id": self.run_id, "rows": [vars(r) for r in self.rows]}, indent=2, default=str
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
        md_path.write_text("\n".join(lines), encoding="utf-8")
        return json_path, md_path
