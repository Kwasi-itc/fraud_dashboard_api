"""
Smoke-tests for the **Case-Management** endpoints.

Run:

    python scripts\case_management_test.py
"""
import requests
from common import BASE_URL, pretty_print


def get_open_cases() -> None:
    pretty_print(requests.get(f"{BASE_URL}/cases/open"))


def get_closed_cases() -> None:
    pretty_print(requests.get(f"{BASE_URL}/cases/closed"))


def get_case(transaction_id: str) -> None:
    pretty_print(
        requests.get(f"{BASE_URL}/case", params={"transaction_id": transaction_id})
    )


def main() -> None:
    print("Running Case-Management API smoke tests against:", BASE_URL)

    # Open & closed cases
    get_open_cases()
    get_closed_cases()

    # Individual case — replace with an ID that exists in your environment
    example_txn_id = "REPLACE_WITH_REAL_TRANSACTION_ID"
    get_case(example_txn_id)


if __name__ == "__main__":
    main()
