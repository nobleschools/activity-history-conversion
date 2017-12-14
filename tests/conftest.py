"""
conftest.py
"""

import logging

import pytest


## ensure no requests calls (used by simple_salesforce)
@pytest.fixture(scope="function", autouse=True)
def no_requests(monkeypatch):
    monkeypatch.delattr("requests.sessions.Session.request")


## disable logging
@pytest.fixture(scope="session", autouse=True)
def no_logging():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)
