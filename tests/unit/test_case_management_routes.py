"""
Unit-level router tests for case_management.app.lambda_handler

We patch the domain helpers (create_case, get_case, etc.) to avoid touching
DynamoDB or other AWS services and simply verify that the correct helper is
invoked for a given API Gateway event.

pytest style ‑ run with:

    python -m pytest tests/unit -v
"""
import os
from unittest.mock import patch, MagicMock

import pytest

# --------------------------------------------------------------------------- #
# Ensure required env-vars exist *before* importing the Lambda module so that
# the module-level DynamoDB wiring doesn’t explode during import in CI.
# --------------------------------------------------------------------------- #
os.environ.setdefault("FRAUD_PROCESSED_TRANSACTIONS_TABLE", "DummyTable")

# Import after env setup
from case_management import app as cm_app  # pylint: disable=wrong-import-position


def _basic_event(path: str, method: str = "GET") -> dict:
    """
    Return a minimal API-GW proxy event with path + method set.
    """
    return {
        "path": path,
        "httpMethod": method,
        "body": "{}",
        "queryStringParameters": {},
    }


@pytest.mark.parametrize(
    "path,method,helper",
    [
        ("/case", "POST", "create_case"),
        ("/case", "GET", "get_case"),
        ("/case/status", "PUT", "update_case_status"),
        ("/case/close", "PUT", "close_case"),
        ("/cases/open", "GET", "get_open_cases"),
        ("/cases/closed", "GET", "get_closed_cases"),
        ("/report", "POST", "create_report"),
    ],
)
def test_lambda_handler_route_mapping(path, method, helper):
    """
    For every (path, method) permutation verify lambda_handler delegates to the
    expected helper function and returns its response unchanged.
    """
    sentinel = {"statusCode": 200, "body": '{"ok": true}'}

    with patch.object(cm_app, helper, return_value=sentinel) as mocked_helper:
        result = cm_app.lambda_handler(_basic_event(path, method), None)

    mocked_helper.assert_called_once()
    # lambda_handler should proxy the exact dict returned by the helper
    assert result == sentinel


def test_lambda_handler_unknown_route():
    """
    Non-existent route should yield a 404 response.
    """
    result = cm_app.lambda_handler(_basic_event("/does/not/exist", "GET"), None)
    assert result["statusCode"] == 404
