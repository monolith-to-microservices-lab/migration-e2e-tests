"""E2E failure/chaos tests against the REAL lab containers. Every test stops
a specific container with `docker stop` (never `down`, never `-v`) and
GUARANTEES it is started again in a `finally` block, even if the assertions
in between fail. No volumes, offsets, replication slot, or publication are
ever touched.

These are slow (each involves real container start/stop + real recovery
polling) and have real side effects on shared lab infrastructure - run them
deliberately: `pytest -m failure`.
"""

from __future__ import annotations

import time

import httpx
import psycopg
import pytest

from tests.helpers import (
    LEGACY_DSN,
    USER_DSN,
    api_post,
    container_log_tail,
    docker_start,
    docker_stop,
    legacy_execute,
    poll_for_row,
    poll_until,
    wait_container_healthy,
)

pytestmark = [pytest.mark.e2e, pytest.mark.failure, pytest.mark.slow]

USER_POSTGRES_CONTAINER = "user-service-user-postgres-1"
USER_CDC_CONTAINER = "user-service-user-service-cdc-1"
KAFKA_CONTAINER = "cdc-kafka"
CONNECT_CONTAINER = "cdc-connect"
KAFKA_EXPORTER_URL = "http://localhost:9308/metrics"


def _consumer_group_lag(consumergroup: str) -> int:
    r = httpx.get(KAFKA_EXPORTER_URL, timeout=5)
    r.raise_for_status()
    total = 0
    for line in r.text.splitlines():
        if (
            line.startswith("kafka_consumergroup_lag{")
            and f'consumergroup="{consumergroup}"' in line
        ):
            total += int(float(line.rsplit(" ", 1)[1]))
    return total


def test_destination_database_down_then_recovers(run_id):
    """1. healthy baseline -> 2. stop user-postgres -> 3. create user ->
    4. Kafka event exists, consumer fails to apply, lag > 0 -> 5. restart
    user-postgres -> 6. consumer recovers -> 7. user appears -> 8. lag -> 0.
    """
    unique_name = f"{run_id}_dbdown"
    lag_before = _consumer_group_lag("user-service-cdc")

    docker_stop(USER_POSTGRES_CONTAINER)
    try:
        resp = api_post("/users", {"name": unique_name})
        assert resp.status_code == 201, resp.text
        user_id = resp.json()["id"]

        # The event reaches Kafka fine (Debezium doesn't care about the
        # destination DB) - lag must rise because the consumer can't apply it.
        def _lag_rose() -> bool:
            return _consumer_group_lag("user-service-cdc") > lag_before

        poll_until(
            _lag_rose,
            timeout=20,
            interval=1.0,
            desc="consumer lag to rise while destination DB is down",
        )

        def _saw_apply_error() -> bool:
            log = container_log_tail(USER_CDC_CONTAINER, lines=100)
            return "cdc.apply_failed" in log or "connect" in log.lower()

        poll_until(
            _saw_apply_error,
            timeout=15,
            interval=1.0,
            desc="an apply/connection error in consumer logs",
        )
    finally:
        docker_start(USER_POSTGRES_CONTAINER)
        wait_container_healthy(USER_POSTGRES_CONTAINER, timeout=60)

    # Recovery: once the DB is back, the already-queued event gets applied
    # (the consumer keeps retrying/redelivering - offset was never committed).
    poll_for_row(USER_DSN, "SELECT id FROM users WHERE id = %s", (user_id,), timeout=30)

    def _lag_returned_to_zero() -> bool:
        return _consumer_group_lag("user-service-cdc") <= lag_before

    poll_until(
        _lag_returned_to_zero, timeout=30, interval=1.0, desc="consumer lag to return to baseline"
    )


def test_consumer_down_then_recovers(run_id):
    """1. stop user-service-cdc -> 2. create 3 users -> 3. confirm lag grows
    -> 4. confirm none applied yet -> 5. start consumer -> 6. all 3 applied
    -> 7. lag back to 0.
    """
    lag_before = _consumer_group_lag("user-service-cdc")
    names = [f"{run_id}_consumerdown_{i}" for i in range(3)]
    user_ids: list[int] = []

    docker_stop(USER_CDC_CONTAINER)
    try:
        for name in names:
            resp = api_post("/users", {"name": name})
            assert resp.status_code == 201, resp.text
            user_ids.append(resp.json()["id"])

        def _lag_grew_by_three() -> bool:
            return _consumer_group_lag("user-service-cdc") >= lag_before + 3

        poll_until(
            _lag_grew_by_three,
            timeout=20,
            interval=1.0,
            desc="lag to grow by 3 while consumer is down",
        )

        with psycopg.connect(USER_DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM users WHERE id = ANY(%s)", (user_ids,))
            assert cur.fetchone()[0] == 0, "consumer is down - nothing should be applied yet"
    finally:
        docker_start(USER_CDC_CONTAINER)

    # Observed real behavior: right after `docker start`, the container can
    # hit a one-time DNS race (Docker's embedded resolver not yet ready for
    # "user-postgres"), crash, and get relaunched by `restart: unless-stopped`
    # - adding ~20-30s before it actually starts consuming. Not a bug in the
    # consumer; a real container-restart timing characteristic of this Docker
    # Compose network, so the timeout here is generous on purpose.
    for uid in user_ids:
        poll_for_row(USER_DSN, "SELECT id FROM users WHERE id = %s", (uid,), timeout=60)

    def _lag_returned_to_zero() -> bool:
        return _consumer_group_lag("user-service-cdc") <= lag_before

    poll_until(
        _lag_returned_to_zero, timeout=60, interval=1.0, desc="consumer lag to return to baseline"
    )


def test_kafka_broker_down_then_recovers(run_id):
    """Stops the broker itself (shared by BOTH consumers and Debezium).
    Documents observed behavior rather than assuming a specific one - this
    combination (Debezium + confluent-kafka client reconnect behavior) is
    exactly the kind of thing worth seeing for real instead of guessing.
    """
    unique_name = f"{run_id}_kafkadown"

    docker_stop(KAFKA_CONTAINER)
    try:
        # The legacy write itself still succeeds (Postgres doesn't need Kafka) -
        # it just can't be streamed out while the broker is down.
        resp = api_post("/users", {"name": unique_name})
        assert resp.status_code == 201, resp.text
        user_id = resp.json()["id"]
        time.sleep(3)  # let Debezium/the consumers observe the broker is gone
        connect_log = container_log_tail(CONNECT_CONTAINER, lines=50)
        consumer_log = container_log_tail(USER_CDC_CONTAINER, lines=50)
        print(
            f"\n--- Observed Kafka Connect log tail during broker outage ---\n{connect_log[-1000:]}"
        )
        print(
            "\n--- Observed user-service-cdc log tail during broker outage ---\n"
            f"{consumer_log[-1000:]}"
        )
    finally:
        docker_start(KAFKA_CONTAINER)
        wait_container_healthy(KAFKA_CONTAINER, timeout=90)

    # Recovery: once the broker is back, Debezium resumes streaming (it never
    # lost anything - it reads from the replication slot, not from Kafka) and
    # the consumer reconnects on its own (confluent-kafka client behavior).
    poll_for_row(USER_DSN, "SELECT id FROM users WHERE id = %s", (user_id,), timeout=60)


def test_kafka_connect_down_then_recovers(run_id):
    """Stops Kafka Connect (Debezium) only - Kafka and the consumers stay up.
    WAL accumulates while Connect is down; measures the lag, then confirms
    the slot resumes cleanly from the correct LSN with no data loss.
    """
    unique_name = f"{run_id}_connectdown"

    def _wal_lag_bytes() -> int:
        with psycopg.connect(LEGACY_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                "FROM pg_replication_slots WHERE slot_name = 'legacy_cdc_slot'"
            )
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0

    lag_before_outage = _wal_lag_bytes()

    docker_stop(CONNECT_CONTAINER)
    try:
        resp = api_post("/users", {"name": unique_name})
        assert resp.status_code == 201, resp.text
        user_id = resp.json()["id"]
        time.sleep(2)

        lag_during_outage = _wal_lag_bytes()
        print(
            f"\nWAL lag during Kafka Connect outage: {lag_during_outage} bytes "
            f"(baseline was {lag_before_outage})"
        )
        assert lag_during_outage >= lag_before_outage  # WAL is accumulating, not being lost
    finally:
        docker_start(CONNECT_CONTAINER)
        wait_container_healthy(CONNECT_CONTAINER, timeout=60)

    # Recovery: connector resumes from the slot's LSN - the earlier write is
    # not lost, it arrives once Connect is back.
    poll_for_row(USER_DSN, "SELECT id FROM users WHERE id = %s", (user_id,), timeout=45)

    def _slot_active_again() -> bool:
        with psycopg.connect(LEGACY_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = 'legacy_cdc_slot'"
            )
            row = cur.fetchone()
            return bool(row and row[0])

    poll_until(
        _slot_active_again, timeout=30, interval=1.0, desc="legacy_cdc_slot to become active again"
    )
    legacy_execute("SELECT 1")  # sanity: legacy DB itself is fine throughout
