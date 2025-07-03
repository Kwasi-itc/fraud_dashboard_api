"""
Quick smoke-tests for the FraudPy Dashboard API.

Make a copy of `.env.example` → `.env` and set `BASE_URL`
to the deployed API Gateway URL, e.g.:

    BASE_URL=https://xxxxxxxx.execute-api.eu-west-1.amazonaws.com/Prod
"""
import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load environment variables from .env in repo root (if present)
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

BASE_URL = os.getenv("BASE_URL")
if not BASE_URL:
    raise RuntimeError(
        "BASE_URL not set.  Create a .env file with a line like:\n"
        "BASE_URL=https://your-api-id.execute-api.region.amazonaws.com/Prod"
    )


def _pretty_print(resp: requests.Response) -> None:
    """Utility to display request / response info nicely."""
    print(f"\n{resp.request.method} {resp.url} → {resp.status_code}")
    try:
        print(json.dumps(resp.json(), indent=2))
    except ValueError:
        print(resp.text)


# ---------- Case-management endpoints ---------- #
def get_open_cases() -> None:
    _pretty_print(requests.get(f"{BASE_URL}/cases/open"))


def get_closed_cases() -> None:
    _pretty_print(requests.get(f"{BASE_URL}/cases/closed"))


def get_case(transaction_id: str) -> None:
    _pretty_print(
        requests.get(f"{BASE_URL}/case", params={"transaction_id": transaction_id})
    )


# ---------- Evaluated-transactions endpoints ---------- #
def get_evaluated_transactions(
    start_date: str, end_date: str, query_type: str = "all", channel: str = ""
) -> None:
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "query_type": query_type,
        "channel": channel,
    }
    _pretty_print(requests.get(f"{BASE_URL}/evaluated-transactions", params=params))


# ---------- Main runner ---------- #
def main() -> None:
    """
    Run a minimal set of endpoint smoke-tests.

    Adjust the sample arguments below to match data that exists in your environment.
    """
    print("Running FraudPy Dashboard API smoke tests against:", BASE_URL)

    # Case-management
    get_open_cases()
    get_closed_cases()

    # Evaluated transactions – adjust dates to match your dataset
    get_evaluated_transactions("2024-01-01", "2024-01-31")


if __name__ == "__main__":
    main()
