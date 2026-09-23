"""E2E: a user and a sale referencing it, both propagated to their own
services, with IDs preserved. Exercises the cross-domain path (Kafka
branches into two independent topics/consumers from the same monolith) in
one scenario.
"""

from __future__ import annotations

import httpx
import pytest

from tests.helpers import MONOLITH_URL, SALES_DSN, USER_DSN, poll_for_row


@pytest.mark.e2e
@pytest.mark.smoke
def test_user_and_linked_sale_both_propagate_with_ids_preserved(run_id):
    user_resp = httpx.post(
        f"{MONOLITH_URL}/users", json={"name": f"{run_id}_combined_user"}, timeout=10
    )
    assert user_resp.status_code == 201, user_resp.text
    user_id = user_resp.json()["id"]

    sale_resp = httpx.post(
        f"{MONOLITH_URL}/sales",
        json={"user_id": user_id, "item_name": f"{run_id}_combined_sale", "quantity": 1},
        timeout=10,
    )
    assert sale_resp.status_code == 201, sale_resp.text
    sale_id = sale_resp.json()["id"]

    user_row = poll_for_row(USER_DSN, "SELECT id FROM users WHERE id = %s", (user_id,))
    assert user_row == (user_id,)

    sale_row = poll_for_row(SALES_DSN, "SELECT id, user_id FROM sales WHERE id = %s", (sale_id,))
    assert sale_row == (sale_id, user_id)
