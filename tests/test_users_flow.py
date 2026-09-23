"""E2E happy path: create/update/delete a user through the REAL stack.

MONOLITH -> LEGACY POSTGRES -> WAL -> DEBEZIUM -> KAFKA -> user-service-cdc
-> USER POSTGRES. No mocks anywhere in this chain.
"""

from __future__ import annotations

import time

import httpx
import pytest

from tests.helpers import (
    MONOLITH_URL,
    USER_DSN,
    legacy_execute,
    poll_for_absence,
    poll_for_row,
)

CONSUMER_CONTAINER = "user-service-user-service-cdc-1"


@pytest.mark.e2e
@pytest.mark.smoke
def test_user_create_update_delete_propagates_end_to_end(evidence, run_id):
    unique_name = f"{run_id}_user_create"

    # --- CREATE via the real monolith API -----------------------------------
    t0 = time.time()
    resp = httpx.post(f"{MONOLITH_URL}/users", json={"name": unique_name}, timeout=10)
    assert resp.status_code == 201, resp.text
    user_id = resp.json()["id"]

    row = poll_for_row(USER_DSN, "SELECT id, name FROM users WHERE id = %s", (user_id,))
    assert row == (user_id, unique_name)
    ev = evidence.from_consumer_log(CONSUMER_CONTAINER, "users", "CREATE", "user", user_id, "c")
    ev.end_to_end_latency_s = time.time() - t0

    # --- UPDATE: direct SQL against the legacy DB ---------------------------
    # (the monolith has no PUT/PATCH /users endpoint - verified, not assumed;
    # this is exactly how Debezium sees any change regardless of how it was made)
    updated_name = f"{unique_name}_updated"
    t1 = time.time()
    legacy_execute("UPDATE users SET name = %s WHERE id = %s", (updated_name, user_id))

    row = poll_for_row(
        USER_DSN, "SELECT id, name FROM users WHERE id = %s AND name = %s", (user_id, updated_name)
    )
    assert row is not None
    ev = evidence.from_consumer_log(CONSUMER_CONTAINER, "users", "UPDATE", "user", user_id, "u")
    ev.end_to_end_latency_s = time.time() - t1

    # --- DELETE: direct SQL against the legacy DB ---------------------------
    t2 = time.time()
    legacy_execute("DELETE FROM users WHERE id = %s", (user_id,))

    poll_for_absence(USER_DSN, "SELECT id FROM users WHERE id = %s", (user_id,))
    ev = evidence.from_consumer_log(CONSUMER_CONTAINER, "users", "DELETE", "user", user_id, "d")
    ev.end_to_end_latency_s = time.time() - t2

    # cleanup note: the DELETE above already IS the cleanup - no further
    # action needed, and no TRUNCATE/mass delete is ever used (see the
    # sales-service incident this whole suite's DB-safety story is about).
