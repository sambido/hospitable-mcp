#!/usr/bin/env python3
"""
Weekly recycling reminder for 65th and 8th.
Runs every Sunday at 4:46pm (1 min after the base trash message from Hospitable).

Checks Seattle's collection API to determine if it's a recycling week,
checks if the property is occupied, and sends the appropriate follow-up
message via Hospitable.
"""

import json, os, ssl, urllib.request, datetime, time

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

MSG_RECYCLING_ON = (
    "It looks like it's a recycling week so all the bins should go out"
)
MSG_RECYCLING_OFF = (
    "It's an off-week for recycling, so just the black and green bins, "
    "(not blue) thanks!"
)

# --- Setup ---
# Load token -- env var first (GitHub Actions), then local .env
HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
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
    """POST to Seattle utilities API."""
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
    """Check Seattle API to see if this week has recycling collection."""
    # 1. Find address
    r = seattle_post("/rest/serviceorder/findaddress", {
        "address": {"addressLine1": address, "city": "", "zip": ""}
    })
    prem_code = r["address"][0]["premCode"]

    # 2. Find account
    r = seattle_post("/rest/serviceorder/findAccount", {
        "address": {"premCode": prem_code}
    })
    account_number = r["account"]["accountNumber"]

    # 3. Guest auth
    r = seattle_post("/rest/auth/guest", {
        "grant_type": "password", "username": "guest", "password": "guest"
    }, use_json=False)
    token = r["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 4. SW Summary
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

    # 5. Calendar
    recycle_service = next((s for s in services if s["description"] == "Recycle"), None)
    if not recycle_service:
        print(f"  No recycling service found for {address}")
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

    # Check if tomorrow (collection day) is in the recycling calendar
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    sp_id = recycle_service["servicePointId"]
    recycling_dates = [
        datetime.datetime.strptime(d, "%m/%d/%Y").date()
        for d in r["calendar"][sp_id]
    ]

    return tomorrow in recycling_dates


def hospitable_get(endpoint, params=None):
    """GET from Hospitable API."""
    url = f"{HOSPITABLE_BASE}{endpoint}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {HOSPITABLE_PAT}", "Accept": "application/json"}
    )
    resp = urllib.request.urlopen(req, context=CTX)
    return json.loads(resp.read())


def hospitable_post(endpoint, payload):
    """POST to Hospitable API."""
    req = urllib.request.Request(
        f"{HOSPITABLE_BASE}{endpoint}",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {HOSPITABLE_PAT}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    resp = urllib.request.urlopen(req, context=CTX)
    return json.loads(resp.read())


def get_active_reservation(property_uuid):
    """Find an active reservation at this property for today."""
    today = datetime.date.today().isoformat()
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()

    # Get reservations in a narrow window around today
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
            return res["id"], name

    return None, None


def send_hospitable_message(reservation_uuid, message):
    """Send a message to a guest via Hospitable."""
    return hospitable_post(f"/reservations/{reservation_uuid}/messages", {
        "body": message,
    })


def run_property(property_key):
    """Run recycling check + message for a single property."""
    config = PROPERTIES[property_key]
    today = datetime.date.today()
    print(f"=== Recycling Reminder: {property_key} | {today.strftime('%A %B %d, %Y')} ===")
    print(f"  Address: {config['address']}")
    print(f"  Collection day: {config['collection_day']}")

    # Check recycling schedule
    try:
        recycling = is_recycling_week(config["address"])
        print(f"  Recycling week: {'YES' if recycling else 'NO'}")
    except Exception as e:
        print(f"  Error checking Seattle API: {e}")
        return

    # Check occupancy
    res_id, guest_name = get_active_reservation(config["hospitable_uuid"])
    if not res_id:
        print(f"  No active reservation -- skipping")
        return

    print(f"  Occupied by: {guest_name} (res: {res_id})")

    # Dedup: check if we already sent a recycling message this week
    try:
        msgs = hospitable_get(f"/reservations/{res_id}/messages", {"per_page": "10"})
        for m in msgs.get("data", []):
            body = (m.get("body", "") or "").lower()
            sent_at = m.get("created_at", "")[:10]
            days_ago = (today - datetime.date.fromisoformat(sent_at)).days if sent_at else 999
            if days_ago <= 2 and ("recycling" in body or "bins" in body):
                print(f"  Already sent recycling message {days_ago}d ago -- skipping")
                return
    except Exception as e:
        print(f"  Warning: could not check for existing message: {e}")

    # Send the right message
    msg = MSG_RECYCLING_ON if recycling else MSG_RECYCLING_OFF
    try:
        send_hospitable_message(res_id, msg)
        print(f"  Sent: \"{msg}\"")
    except Exception as e:
        print(f"  Error sending message: {e}")

    print("Done!")


def main():
    """Run for a specific property via command line arg, or all."""
    import sys
    if len(sys.argv) > 1:
        prop = sys.argv[1]
        if prop in PROPERTIES:
            run_property(prop)
        else:
            print(f"Unknown property: {prop}. Options: {list(PROPERTIES.keys())}")
    else:
        for key in PROPERTIES:
            run_property(key)
            time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import sys
        print(f"\nFATAL: {e}")
        sys.exit(1)
