#!/usr/bin/env python3
"""
ButterflyMX Code Automation — Chad's Phinney Flat (#302)

2 days before check-in at 11am PT: texts building manager to set up
ButterflyMX intercom code with guest details.

Guest phone from Hospitable, guest email from Notion Guest Contacts.
"""

import json, os, ssl, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    PACIFIC = ZoneInfo("America/Los_Angeles")
except ImportError:
    PACIFIC = timezone(timedelta(hours=-7))

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
QUO_API_KEY = os.environ.get("QUO_API_KEY", "")

if not HOSPITABLE_PAT or not NOTION_TOKEN:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
        HOSPITABLE_PAT = HOSPITABLE_PAT or os.environ.get("HOSPITABLE_PAT", "")
    except ImportError:
        pass
    if not NOTION_TOKEN:
        try:
            with open(os.path.expanduser("~/.claude.json")) as f:
                cfg = json.load(f)
            h = cfg["mcpServers"]["notionApi"]["env"]["OPENAPI_MCP_HEADERS"]
            NOTION_TOKEN = json.loads(h)["Authorization"].replace("Bearer ", "")
        except (FileNotFoundError, KeyError):
            pass

CTX = ssl.create_default_context()

# Chad's property
CHAD_PROPERTY_UUID = "eefb5918-5149-4b4e-bdd0-277754409cb0"
BUILDING_MANAGER_PHONE = "+12537320947"

# Quo
QUO_FROM = "PNI52JLEHJ"  # eSam Automations

# Notion
GUEST_CONTACTS_DB = "32950c17-99cc-810b-b234-e3f653240342"

HOSPITABLE_BASE = "https://public.api.hospitable.com/v2"


# --------------------------------------------------------------------------- #
# API helpers
# --------------------------------------------------------------------------- #
def hospitable_get(endpoint, params=None):
    url = f"{HOSPITABLE_BASE}{endpoint}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {HOSPITABLE_PAT}",
        "Accept": "application/json",
    })
    resp = urllib.request.urlopen(req, context=CTX)
    return json.loads(resp.read())


def notion_request(method, endpoint, data=None):
    url = f"https://api.notion.com/v1{endpoint}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method, headers={
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    })
    try:
        resp = urllib.request.urlopen(req, context=CTX)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"  Notion error {e.code}: {e.read().decode()[:200]}")
        return None


def send_text(to, message):
    if not QUO_API_KEY:
        print(f"  [DRY RUN] Would text {to}: {message}")
        return True

    body = json.dumps({
        "content": message,
        "from": QUO_FROM,
        "to": [to],
    }).encode()

    req = urllib.request.Request(
        "https://api.openphone.com/v1/messages",
        data=body,
        headers={
            "Authorization": QUO_API_KEY,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, context=CTX)
        result = json.loads(resp.read())
        print(f"  Text sent to {to}: {result.get('data', {}).get('id', 'ok')}")
        return True
    except urllib.error.HTTPError as e:
        print(f"  Quo error {e.code}: {e.read().decode()[:200]}")
        return False


# --------------------------------------------------------------------------- #
# Guest email lookup from Notion Guest Contacts
# --------------------------------------------------------------------------- #
def get_guest_email(guest_name):
    """Look up guest email from Guest Contacts DB by name + property."""
    # Try exact match first, then contains match
    for filter_type in ["equals", "contains"]:
        result = notion_request("POST", f"/databases/{GUEST_CONTACTS_DB}/query", {
            "filter": {
                "and": [
                    {"property": "Name", "title": {filter_type: guest_name}},
                    {"property": "Property", "select": {"equals": "Chad"}},
                ]
            },
            "page_size": 1,
            "sorts": [{"property": "Check-in", "direction": "descending"}],
        })
        if result and result.get("results"):
            email = result["results"][0]["properties"].get("Email", {}).get("email", "")
            if email:
                return email
    return ""


# --------------------------------------------------------------------------- #
# Date formatting
# --------------------------------------------------------------------------- #
def friendly_date(date_str):
    """'2026-04-06' -> 'Monday, April 6th'"""
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    day_of_week = dt.strftime("%A")
    month_day = dt.strftime("%B %-d")
    day_num = dt.day
    suffix = "th" if 11 <= day_num <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day_num % 10, "th")
    return f"{day_of_week}, {month_day}{suffix}"


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    now_pacific = datetime.now(PACIFIC)
    today = now_pacific.date()
    current_hour = now_pacific.hour
    print(f"=== ButterflyMX Code Check: {today} {now_pacific.strftime('%-I:%M %p')} PT ===")

    # Only act during the 11am PT window (10-12)
    if not (10 <= current_hour <= 12):
        print(f"Outside 11am window ({current_hour}:00 PT), skipping")
        return

    # Look at check-ins 2 days from now
    target_date = (today + timedelta(days=2)).isoformat()
    start = target_date
    end = (today + timedelta(days=3)).isoformat()

    try:
        res_resp = hospitable_get("/reservations", {
            "properties[]": CHAD_PROPERTY_UUID,
            "include": "guest",
            "per_page": "50",
            "start_date": start,
            "end_date": end,
            "date_query": "checkin",
        })
    except Exception as e:
        print(f"Error fetching reservations: {e}")
        return

    reservations = res_resp.get("data", [])
    print(f"Check-ins on {target_date}: {len(reservations)}")

    for res in reservations:
        if res.get("status") not in ("accepted",):
            continue

        checkin = res.get("arrival", res.get("check_in", ""))[:10]
        checkout = res.get("departure", res.get("check_out", ""))[:10]
        if not checkin or not checkout:
            continue

        guest = res.get("guest", {}) or {}
        full_name = f"{guest.get('first_name', '')} {guest.get('last_name', '')}".strip() or "Guest"
        # Phone is in phone_numbers array, format as +1XXXXXXXXXX
        phone_numbers = guest.get("phone_numbers", []) or []
        raw_phone = phone_numbers[0] if phone_numbers else ""
        guest_phone = f"+{raw_phone}" if raw_phone and not raw_phone.startswith("+") else raw_phone

        # Look up email from Guest Contacts
        guest_email = get_guest_email(full_name)

        # Format dates
        checkin_pretty = friendly_date(checkin)
        checkout_pretty = friendly_date(checkout)

        # Build message
        lines = [
            f"Set ButterflyMX code for guest {full_name}",
            f"Check-in: {checkin_pretty}",
            f"Checkout: {checkout_pretty}",
        ]
        if guest_phone:
            lines.append(f"Guest Phone: {guest_phone}")
        else:
            lines.append("Guest Phone: not available")
        if guest_email:
            lines.append(f"Guest Email: {guest_email}")
        else:
            lines.append("Guest Email: not available")

        message = "\n".join(lines)

        print(f"  {full_name} | check-in {checkin} | checkout {checkout}")
        print(f"  Phone: {guest_phone or 'N/A'} | Email: {guest_email or 'N/A'}")

        send_text(BUILDING_MANAGER_PHONE, message)

    print("Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import sys, traceback
        print(f"\nFATAL: {e}")
        traceback.print_exc()
        sys.exit(1)
