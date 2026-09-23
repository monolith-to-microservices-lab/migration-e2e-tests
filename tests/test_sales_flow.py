"""E2E happy path: create/update/delete a sale through the REAL stack.
Mirrors test_users_flow.py; MONOLITH -> ... -> sales-service-cdc -> SALES DB.
"""

from __future__ import annotations

import time

import httpx
import pytest

from tests.helpers import (
    MONOLITH_URL,
    SALES_DSN,
    legacy_execute,
    poll_for_absence,
    poll_for_row,
)

CONSUMER_CONTAINER = "sales-service-sales-service-cdc-1"


@pytest.mark.e2e
@pytest.mark.smoke
def test_sale_create_update_delete_propagates_end_to_end(evidence, run_id):
    item_name = f"{run_id}_sale_create"

    t0 = time.time()
    resp = httpx.post(
        f"{MONOLITH_URL}/sales",
        json={"user_id": 1, "item_name": item_name, "quantity": 3},
        timeout=10,
    )
    assert resp.status_code == 201, resp.text
    sale_id = resp.json()["id"]

    row = poll_for_row(
        SALES_DSN, "SELECT id, item_name, quantity FROM sales WHERE id = %s", (sale_id,)
    )
    assert row == (sale_id, item_name, 3)
    ev = evidence.from_consumer_log(CONSUMER_CONTAINER, "sales", "CREATE", "sale", sale_id, "c")
    ev.end_to_end_latency_s = time.time() - t0

    t1 = time.time()
    legacy_execute("UPDATE sales SET quantity = 42 WHERE id = %s", (sale_id,))
    row = poll_for_row(
        SALES_DSN, "SELECT id FROM sales WHERE id = %s AND quantity = 42", (sale_id,)
    )
    assert row is not None
    ev = evidence.from_consumer_log(CONSUMER_CONTAINER, "sales", "UPDATE", "sale", sale_id, "u")
    ev.end_to_end_latency_s = time.time() - t1

    t2 = time.time()
    legacy_execute("DELETE FROM sales WHERE id = %s", (sale_id,))
    poll_for_absence(SALES_DSN, "SELECT id FROM sales WHERE id = %s", (sale_id,))
    ev = evidence.from_consumer_log(CONSUMER_CONTAINER, "sales", "DELETE", "sale", sale_id, "d")
    ev.end_to_end_latency_s = time.time() - t2
