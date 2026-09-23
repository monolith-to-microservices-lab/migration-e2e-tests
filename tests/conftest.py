from __future__ import annotations

import pytest

from tests.helpers import EvidenceCollector, new_run_id


@pytest.fixture(scope="session")
def run_id() -> str:
    return new_run_id()


@pytest.fixture(scope="session")
def evidence(run_id):
    collector = EvidenceCollector(run_id=run_id)
    yield collector
    json_path, md_path = collector.write()
    print(f"\nE2E evidence written to:\n  {json_path}\n  {md_path}")


def pytest_collection_modifyitems(config, items):
    for item in items:
        item.add_marker(pytest.mark.e2e)
