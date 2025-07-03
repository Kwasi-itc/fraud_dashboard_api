"""
Smoke-tests for the **Evaluated-Transactions** endpoint.

Run:

    python scripts\evaluated_transactions_test.py
"""
import requests
from common import BASE_URL, pretty_print


def get_evaluated_transactions(
    start_date: str,
    end_date: str,
    query_type: str = "all",
    channel: str = "",
) -> None:
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "query_type": query_type,
        "channel": channel,
    }
    pretty_print(requests.get(f"{BASE_URL}/evaluated-transactions", params=params))


def main() -> None:
    print("Running Evaluated-Transactions API smoke tests against:", BASE_URL)

    # Adjust dates / parameters to match data in your DynamoDB table
    get_evaluated_transactions("2024-01-01", "2024-01-31")
    get_evaluated_transactions("2024-01-01", "2024-01-31", query_type="normal")
    get_evaluated_transactions("2024-01-01", "2024-01-31", query_type="affected")


if __name__ == "__main__":
    main()
