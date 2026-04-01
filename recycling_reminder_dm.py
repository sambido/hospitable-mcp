#!/usr/bin/env python3
"""
Recycling reminder — DMs Sam via Slack with actionable info.

Checks Seattle recycling API + Hospitable occupancy, then sends:
- Recycling on/off week
- Occupied → "remind guest" with guest name + checkout date
- Vacant → "roll bins out yourself"
"""

import json, os, ssl, sys, urllib.request, datetime

# --- Config ---
PROPERTIES = {
    "65th": {
        "hospitable_uuid": "92a1c198-4d3e-4d1b-a5f8-e90f98f1c49c",
        "address": "1025 NW 65TH ST",
        "collection_day": "Monday",
    },
    "8th": {
        "hospitable_uuid": "f3fd4981-3f21-4c5a-8888-ba259834ddb5",
        "address": "7301 8TH AVE NW",
        "collection_day": "Tuesday",
    },
}

# --- Setup ---
HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
SLACK_DM_WEBHOOK_URL = os.environ.get("SLACK_DM_WEBHOOK_URL", "")

if not HOSPITABLE_PAT:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
        HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
    except ImportError:
        pass

CTX = ssl.create_default_context()
SEATTLE_BASE = "https://myutilities.seattle.gov"
HOSPITABLE_BASE = "https://public.api.hospitable.com/v2"


def seattle_post(endpoint, payload, headers=None, use_json=True):
    hdrs = {"Accept": "application/json"}
    if use_json:
        hdrs["Content-Type"] = "application/json"
        body = json.dumps(payload).encode()
    else:
        body = "&".join(f"{k}={v}" for k, v in payload.items()).encode()
        hdrs["Content-Type"] = "application/x-www-form-urlencoded"
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(
        f"{SEATTLE_BASE}{endpoint}", data=body, headers=hdrs, method="POST"
    )
    resp = urllib.request.urlopen(req, context=CTX)
    return json.loads(resp.read())


def is_recycling_week(address):
    r = seattle_post("/rest/serviceorder/findaddress", {
        "address": {"addressLine1": address, "city": "", "zip": ""}
    })
    prem_code = r["address"][0]["premCode"]

    r = seattle_post("/rest/serviceorder/findAccount", {
        "address": {"premCode": prem_code}
    })
    account_number = r["account"]["accountNumber"]

    r = seattle_post("/rest/auth/guest", {
        "grant_type": "password", "username": "guest", "password": "guest"
    }, use_json=False)
    token = r["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    r = seattle_post("/rest/guest/swsummary", {
        "customerId": "guest",
        "accountContext": {
            "accountNumber": account_number,
            "personId": None,
            "companyCd": None,
            "serviceAddress": None,
        },
    }, headers=headers)

    services = r["accountSummaryType"]["swServices"][0]["services"]
    person_id = r["accountContext"]["personId"]
    company_cd = r["accountContext"]["companyCd"]

    recycle_service = next((s for s in services if s["description"] == "Recycle"), None)
    if not recycle_service:
        return False

    service_points = [s["servicePointId"] for s in services]
    r = seattle_post("/rest/solidwastecalendar", {
        "customerId": "guest",
        "accountContext": {
            "accountNumber": account_number,
            "personId": person_id,
            "companyCd": company_cd,
        },
        "servicePoints": service_points,
    }, headers=headers)

    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    sp_id = recycle_service["servicePointId"]
    recycling_dates = [
        datetime.datetime.strptime(d, "%m/%d/%Y").date()
        for d in r["calendar"][sp_id]
    ]
    return tomorrow in recycling_dates


def hospitable_get(endpoint, params=None):
    url = f"{HOSPITABLE_BASE}{endpoint}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {HOSPITABLE_PAT}", "Accept": "application/json"}
    )
    resp = urllib.request.urlopen(req, context=CTX)
    return json.loads(resp.read())


def get_active_reservation(property_uuid):
    """Find active reservation. Returns (guest_name, checkout_date) or (None, None)."""
    today = datetime.date.today().isoformat()
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
    week_ago = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()

    r = hospitable_get("/reservations", {
        "properties[]": property_uuid,
        "include": "guest",
        "per_page": "50",
        "start_date": week_ago,
        "end_date": tomorrow,
    })

    for res in r.get("data", []):
        arrival = res.get("arrival_date", "")[:10]
        departure = res.get("departure_date", "")[:10]
        status = res.get("status", "")

        if status in ("accepted",) and arrival <= today < departure:
            guest = res.get("guest", {})
            name = f"{guest.get('first_name', '')} {guest.get('last_name', '')}".strip()
            return name, departure

    return None, None


def send_slack_dm(message):
    req = urllib.request.Request(
        SLACK_DM_WEBHOOK_URL,
        data=json.dumps({"text": message}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, context=CTX)


def run_property(prop_key):
    config = PROPERTIES[prop_key]
    collection_day = config["collection_day"]
    today = datetime.date.today()

    print(f"=== {prop_key} | {today} ===")

    # Check recycling
    try:
        recycling = is_recycling_week(config["address"])
    except Exception as e:
        print(f"  Seattle API error: {e}")
        send_slack_dm(
            f":warning: *{prop_key} — Recycling Check Failed*\n"
            f"Couldn't reach Seattle utilities API. Check manually.\n"
            f"Pickup is {collection_day}."
        )
        return

    week_type = "Recycling Week" if recycling else "Trash Only"
    icon = ":recycle:" if recycling else ":wastebasket:"
    bins = "all bins (black, green, blue)" if recycling else "black and green bins only (no blue)"

    # Check occupancy
    guest_name, checkout = get_active_reservation(config["hospitable_uuid"])

    if guest_name:
        checkout_fmt = datetime.datetime.strptime(checkout, "%Y-%m-%d").strftime("%A %b %-d")
        msg = (
            f"{icon} *{prop_key} — {week_type}*\n"
            f"Pickup: {collection_day} morning. Put out {bins}.\n"
            f"Occupied: *{guest_name}* (checkout {checkout_fmt})\n"
            f"Send guest a reminder to roll bins to curb tonight."
        )
    else:
        msg = (
            f"{icon} *{prop_key} — {week_type}*\n"
            f"Pickup: {collection_day} morning. Put out {bins}.\n"
            f"*Vacant* — you need to roll bins out yourself."
        )

    print(f"  {week_type}, {'occupied by ' + guest_name if guest_name else 'vacant'}")
    send_slack_dm(msg)
    print("  DM sent")


def main():
    if not HOSPITABLE_PAT:
        print("ERROR: HOSPITABLE_PAT not set")
        sys.exit(1)
    if not SLACK_DM_WEBHOOK_URL:
        print("ERROR: SLACK_DM_WEBHOOK_URL not set")
        sys.exit(1)

    if len(sys.argv) > 1:
        prop = sys.argv[1]
        if prop in PROPERTIES:
            run_property(prop)
        else:
            print(f"Unknown property: {prop}. Options: {list(PROPERTIES.keys())}")
            sys.exit(1)
    else:
        for key in PROPERTIES:
            run_property(key)


if __name__ == "__main__":
    main()
