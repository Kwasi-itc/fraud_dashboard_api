"""
Unit-level router tests for case_management.app.lambda_handler

We patch the domain helpers (create_case, get_case, etc.) to avoid touching
DynamoDB or other AWS services and simply verify that the correct helper is
invoked for a given API Gateway event.

pytest style ‑ run with:

    python -m pytest tests/unit -v
"""
import os
import json
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


# --------------------------------------------------------------------------- #
#                                                                       Logic #
# --------------------------------------------------------------------------- #

@pytest.fixture()
def fake_table():
    """
    Returns a MagicMock that mimics the DynamoDB Table resource and patches the
    one used inside case_management.app for the duration of the test.
    """
    with patch.object(cm_app, "table", autospec=True) as mock_table:
        yield mock_table


# ------------------------------- create_case ------------------------------- #

def test_create_case_success(fake_table):
    event = {
        "httpMethod": "POST",
        "resource": "/case",
        "body": json.dumps({"transaction_id": "tx1", "assigned_to": "alice", "status": "OPEN"}),
    }

    resp = cm_app.create_case(event, None)

    fake_table.put_item.assert_called_once()
    assert resp["statusCode"] == 200


def test_create_case_missing_id(fake_table):
    event = {"httpMethod": "POST", "resource": "/case", "body": json.dumps({})}

    resp = cm_app.create_case(event, None)

    assert resp["statusCode"] == 400
    fake_table.put_item.assert_not_called()


# ----------------------------- update_case_status -------------------------- #

def test_update_case_status_success(fake_table):
    event = {
        "httpMethod": "PUT",
        "resource": "/case/status",
        "body": json.dumps({"transaction_id": "tx1", "status": "UNDER_REVIEW", "assigned_to": "bob"}),
    }

    resp = cm_app.update_case_status(event, None)

    fake_table.update_item.assert_called_once()
    assert resp["statusCode"] == 200


def test_update_case_status_missing_fields(fake_table):
    event = {"httpMethod": "PUT", "resource": "/case/status", "body": json.dumps({})}

    resp = cm_app.update_case_status(event, None)

    assert resp["statusCode"] == 400
    fake_table.update_item.assert_not_called()


# --------------------------------- get_case -------------------------------- #

def test_get_case_found(fake_table):
    fake_table.get_item.return_value = {
        "Item": {
            "PARTITION_KEY": "CASE",
            "SORT_KEY": "tx1",
            "status": "OPEN",
            "created_at": "2023-01-01T00:00:00",
        }
    }
    event = {"httpMethod": "GET", "resource": "/case", "queryStringParameters": {"transaction_id": "tx1"}}

    resp = cm_app.get_case(event, None)

    fake_table.get_item.assert_called_once()
    assert resp["statusCode"] == 200
    assert "transaction_id" in json.loads(resp["body"])


def test_get_case_not_found(fake_table):
    fake_table.get_item.return_value = {}
    event = {"httpMethod": "GET", "resource": "/case", "queryStringParameters": {"transaction_id": "nope"}}

    resp = cm_app.get_case(event, None)

    assert resp["statusCode"] == 404


# ------------------------------- get_open_cases ---------------------------- #

def test_get_open_cases(fake_table):
    fake_table.query.return_value = {
        "Items": [
            {"PARTITION_KEY": "CASE", "SORT_KEY": "tx1", "status": "OPEN", "created_at": "2023-01-01T00:00:00"}
        ]
    }
    event = {"httpMethod": "GET", "resource": "/cases/open"}

    resp = cm_app.get_open_cases(event, None)

    fake_table.query.assert_called_once()
    assert resp["statusCode"] == 200
    assert "open_cases" in json.loads(resp["body"])


# ------------------------------ close_case --------------------------------- #

def test_close_case_success(fake_table):
    fake_table.get_item.return_value = {
        "Item": {
            "PARTITION_KEY": "CASE",
            "SORT_KEY": "tx1",
            "status": "OPEN",
            "created_at": "2023-01-01T00:00:00",
        }
    }

    event = {"httpMethod": "PUT", "resource": "/case/close", "body": json.dumps({"transaction_id": "tx1"})}

    resp = cm_app.close_case(event, None)

    fake_table.get_item.assert_called_once()
    fake_table.delete_item.assert_called_once()
    fake_table.put_item.assert_called_once()
    assert resp["statusCode"] == 200


def test_close_case_not_found(fake_table):
    fake_table.get_item.return_value = {}
    event = {"httpMethod": "PUT", "resource": "/case/close", "body": json.dumps({"transaction_id": "does_not_exist"})}

    resp = cm_app.close_case(event, None)

    assert resp["statusCode"] == 404
    fake_table.delete_item.assert_not_called()
    fake_table.put_item.assert_not_called()


# ------------------------------- create_report ----------------------------- #

def test_create_report_success(fake_table):
    with patch("case_management.app.uuid") as mock_uuid:
        mock_uuid.uuid4.return_value = "abcd"
        event = {
            "httpMethod": "POST",
            "resource": "/report",
            "body": json.dumps({"transaction_id": "tx1"}),
        }

        resp = cm_app.create_report(event, None)

    fake_table.put_item.assert_called_once()
    assert resp["statusCode"] == 200


def test_create_report_missing_id(fake_table):
    event = {"httpMethod": "POST", "resource": "/report", "body": json.dumps({})}

    resp = cm_app.create_report(event, None)

    assert resp["statusCode"] == 400
    fake_table.put_item.assert_not_called()
