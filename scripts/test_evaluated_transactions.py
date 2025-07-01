"""
Unit tests for evaluated_transactions.app

• Patch table-level helpers so no real AWS calls are made.
• Verify lambda_handler routing + helper behaviour.
Run with:

    python -m pytest scripts/test_evaluated_transactions.py -v
"""
from __future__ import annotations

import os
import json
from datetime import datetime
from unittest.mock import patch

import pytest

# --------------------------------------------------------------------------- #
# Provide dummy env-var before importing the Lambda so boto3 wiring is safe.
# --------------------------------------------------------------------------- #
os.environ.setdefault("FRAUD_PROCESSED_TRANSACTIONS_TABLE", "DummyTable")

from evaluated_transactions import app as et_app  # noqa: E402

# --------------------------------------------------------------------------- #
#                              Helper utilities                               #
# --------------------------------------------------------------------------- #


def _event(query: dict[str, str] | None = None) -> dict:
    """
    Return a minimal API-GW proxy event with supplied query params.
    evaluated_transactions.lambda_handler only reads httpMethod,
    queryStringParameters and ignores path.
    """
    return {
        "httpMethod": "GET",
        "path": "/evaluated-transactions",
        "body": "",
        "queryStringParameters": query or {},
    }


# --------------------------------------------------------------------------- #
#                            lambda_handler tests                             #
# --------------------------------------------------------------------------- #


def test_lambda_handler_missing_dates():
    """
    start_date & end_date are mandatory → expect 400.
    """
    resp = et_app.lambda_handler(_event({}), None)
    assert resp["statusCode"] == 400


@pytest.mark.parametrize("route", ["normal", "all"])
def test_lambda_handler_happy_path(route):
    """
    When query_type is *not* entity_list lambda_handler should delegate to
    query_transactions().
    """
    query = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "query_type": route,
    }
    sentinel = [{"foo": "bar"}]

    with patch.object(
        et_app, "query_transactions", return_value=sentinel
    ) as mocked_qx:
        resp = et_app.lambda_handler(_event(query), None)

    mocked_qx.assert_called_once()
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["data"] == sentinel


def test_lambda_handler_entity_list_path():
    """
    query_type=entity_list should hit query_transactions_by_entity_and_list().
    """
    query = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "query_type": "entity_list",
        "list_type": "blacklist",
        "entity_type": "account",
    }
    sentinel = [{"baz": "qux"}]

    with patch.object(
        et_app, "query_transactions_by_entity_and_list", return_value=sentinel
    ) as mocked_helper:
        resp = et_app.lambda_handler(_event(query), None)

    mocked_helper.assert_called_once()
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"])["data"] == sentinel


# --------------------------------------------------------------------------- #
#                     construct_partition_key param checks                    #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "params,expected",
    [
        ({"query_type": "all"}, "EVALUATED"),
        ({"query_type": "normal"}, "EVALUATED"),
        ({"query_type": "affected"}, "EVALUATED"),
        (
            {"query_type": "account", "channel": "WEB", "account_id": "A1"},
            "EVALUATED-WEB-ACCOUNT-A1",
        ),
        (
            {
                "query_type": "application",
                "channel": "WEB",
                "application_id": "APP1",
            },
            "EVALUATED-WEB-APPLICATION-APP1",
        ),
        (
            {
                "query_type": "merchant",
                "channel": "MOBILE",
                "application_id": "APP1",
                "merchant_id": "M1",
            },
            "EVALUATED-MOBILE-MERCHANT-APP1__M1",
        ),
        (
            {
                "query_type": "product",
                "channel": "WEB",
                "application_id": "APP1",
                "merchant_id": "M1",
                "product_id": "P1",
            },
            "EVALUATED-WEB-PRODUCT-APP1__M1__P1",
        ),
        ({"query_type": "blacklist"}, "EVALUATED-BLACKLIST"),
    ],
)
def test_construct_partition_key(params, expected):
    assert et_app.construct_partition_key(params) == expected


def test_construct_partition_key_invalid():
    with pytest.raises(ValueError):
        et_app.construct_partition_key({"query_type": "does-not-exist"})
