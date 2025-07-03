"""
Shared helpers for the local API smoke-test scripts.

Loads BASE_URL from the `.env` file that lives in the **project root**.
"""
import json
import os
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

# Load environment variables once, as soon as this module is imported
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

BASE_URL: Optional[str] = os.getenv("BASE_URL")
if not BASE_URL:
    raise RuntimeError(
        "BASE_URL not set.  Create a .env file with a line like:\n"
        "BASE_URL=https://your-api-id.execute-api.region.amazonaws.com/Prod/"
    )


def pretty_print(resp: requests.Response) -> None:
    """
    Display an HTTP request/response summary.

    Always uses ASCII characters to avoid Windows console encoding issues.
    """
    print(f"\n{resp.request.method} {resp.url} -> {resp.status_code}")
    try:
        print(json.dumps(resp.json(), indent=2))
    except ValueError:
        print(resp.text)
