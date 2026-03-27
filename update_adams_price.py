#!/usr/bin/env python3
"""One-off script to set Adam's nightly rate to $200 for tonight (2026-03-27)."""

import json, os, ssl, urllib.request

HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
if not HOSPITABLE_PAT:
    try:
        with open(os.path.join(os.path.dirname(__file__), ".env")) as f:
            for line in f:
                if line.startswith("HOSPITABLE_PAT="):
                    HOSPITABLE_PAT = line.strip().split("=", 1)[1]
    except FileNotFoundError:
        pass

if not HOSPITABLE_PAT:
    raise SystemExit("Error: HOSPITABLE_PAT not set. Export it or add to .env")

ADAM_UUID = "14912b54-f5e0-47ac-a8c2-1e1d9e17bbd6"
DATE = "2026-03-27"
PRICE_CENTS = 20000  # $200

url = f"https://public.api.hospitable.com/v2/properties/{ADAM_UUID}/calendar"
payload = [{"date": DATE, "price": {"amount": PRICE_CENTS}}]

ctx = ssl.create_default_context()
req = urllib.request.Request(
    url,
    data=json.dumps(payload).encode(),
    headers={
        "Authorization": f"Bearer {HOSPITABLE_PAT}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    },
    method="PUT",
)

resp = urllib.request.urlopen(req, context=ctx)
print(f"Status: {resp.status}")
print(json.dumps(json.loads(resp.read()), indent=2))
print(f"\nDone — Adam's price set to $200 for {DATE}")
