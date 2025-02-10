import uuid

import pytest


def pytest_addoption(parser):
    parser.addoption("--warehouse-type", action="store", default=None)


@pytest.fixture(autouse=True)
def mock_uuid(monkeypatch):
    """Mock UUID to ensure consistent UUIDs in tests."""

    class MockUUID:
        def __init__(self):
            self.counter = 0

        def __call__(self):
            self.counter += 1
            return uuid.UUID(
                f"00000000-0000-0000-0000-{self.counter:012d}"  # noqa: E231
            )

    mock = MockUUID()
    monkeypatch.setattr(uuid, "uuid4", mock)
    return mock
