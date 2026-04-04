#!/usr/bin/env python3
"""
Gift Card Automation — Chad's Phinney Flat (#302)

For stays 5+ nights at Chad's European Inspired Phinney Flat:
- 1 day before check-in at 11am PT: Text Bryant to send $50 Sea Creatures gift card
- Day of check-in at 12pm PT: Follow-up if Bryant hasn't replied

Runs daily via GitHub Actions.
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
CHAD_PROPERTY_NOTION_ID = "32050c17-99cc-8188-9bfd-f23a4cc8c028"
MIN_NIGHTS = 5

# Bryant (gift card coordinator)
BRYANT_PHONE = "+12065025344"

# Quo (OpenPhone)
QUO_FROM = "PNI52JLEHJ"  # eSam Automations (206) 350-3726

# Notion
ACTION_ITEMS_DB = "33750c17-99cc-81d2-8fc7-c53c747abbc7"
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
    """Send SMS via Quo (OpenPhone) API."""
    if not QUO_API_KEY:
        print(f"  [DRY RUN] Would text {to}: {message}")
        return True

    api_key = QUO_API_KEY.strip()

    body = json.dumps({
        "content": message,
        "from": QUO_FROM,
        "to": [to],
    }).encode()

    req = urllib.request.Request(
        "https://api.openphone.com/v1/messages",
        data=body,
        headers={
            "Authorization": api_key,
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
        err_body = e.read().decode()[:300]
        print(f"  Quo error {e.code}: {err_body}")
        print(f"  API key length: {len(api_key)}, starts with: {api_key[:8]}...")
        return False


def check_for_reply(from_phone, since_hours=24):
    """Check if we received a reply from a phone number in the last N hours."""
    if not QUO_API_KEY:
        print(f"  [DRY RUN] Would check for reply from {from_phone}")
        return False

    api_key = QUO_API_KEY.strip()

    req = urllib.request.Request(
        f"https://api.openphone.com/v1/messages?phoneNumberId={QUO_FROM}&participants={from_phone}&maxResults=5",
        headers={
            "Authorization": api_key,
            "Content-Type": "application/json",
        },
    )
    try:
        resp = urllib.request.urlopen(req, context=CTX)
        result = json.loads(resp.read())
        messages = result.get("data", [])

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()

        for msg in messages:
            # Look for incoming messages from Bryant after our outgoing text
            if msg.get("direction") == "incoming" and msg.get("createdAt", "") > cutoff:
                print(f"  Reply found from {from_phone}: \"{msg.get('content', '')[:80]}\"")
                return True

        print(f"  No reply from {from_phone} in last {since_hours} hours")
        return False
    except urllib.error.HTTPError as e:
        print(f"  Quo error checking replies: {e.code}")
        return False


# --------------------------------------------------------------------------- #
# Guest email lookup from Notion Guest Contacts
# --------------------------------------------------------------------------- #
def get_guest_email(guest_name):
    """Look up guest email from Guest Contacts DB by name + property."""
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
# Check for existing action item (prevent duplicates)
# --------------------------------------------------------------------------- #
def action_item_exists(reservation_code):
    """Check if we already created an action item for this reservation."""
    result = notion_request("POST", f"/databases/{ACTION_ITEMS_DB}/query", {
        "filter": {
            "and": [
                {"property": "Reservation ID", "rich_text": {"equals": reservation_code}},
                {"property": "Item", "title": {"contains": "Sea Creatures gift card"}},
            ]
        },
        "page_size": 1,
    })
    return bool(result and result.get("results"))


# --------------------------------------------------------------------------- #
# Create action item
# --------------------------------------------------------------------------- #
def create_gift_card_action_item(guest_name, checkin_date, reservation_code, nights):
    props = {
        "Item": {"title": [{"text": {"content": f"Sea Creatures gift card for {guest_name} ({nights} nights)"}}]},
        "Type": {"select": {"name": "Request"}},
        "Category": {"multi_select": [{"name": "Booking & Policies"}]},
        "Status": {"select": {"name": "New"}},
        "Priority": {"select": {"name": "Medium"}},
        "Source": {"select": {"name": "Auto-detected"}},
        "Decision": {"select": {"name": "Approved"}},
        "Date Received": {"date": {"start": datetime.now(PACIFIC).strftime("%Y-%m-%d")}},
        "Due Date": {"date": {"start": checkin_date}},
        "Property": {"relation": [{"id": CHAD_PROPERTY_NOTION_ID}]},
        "Outcome Notes": {"rich_text": [{"text": {"content":
            f"$50 Sea Creatures gift card. Guest checking in {checkin_date}. "
            f"Text sent to Bryant at {BRYANT_PHONE}."
        }}]},
    }
    if guest_name:
        props["Guest Name"] = {"rich_text": [{"text": {"content": guest_name}}]}
    if reservation_code:
        props["Reservation ID"] = {"rich_text": [{"text": {"content": reservation_code}}]}

    return notion_request("POST", "/pages", {
        "parent": {"database_id": ACTION_ITEMS_DB},
        "properties": props,
    })


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
    print(f"=== Gift Card Check: {today} {now_pacific.strftime('%-I:%M %p')} PT ===")

    # Look at check-ins in the next 2 days (catches day-before and day-of windows)
    start = today.isoformat()
    end = (today + timedelta(days=2)).isoformat()

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
    print(f"Upcoming check-ins at Chad's: {len(reservations)}")

    for res in reservations:
        if res.get("status") not in ("accepted",):
            continue

        checkin = res.get("arrival", res.get("check_in", ""))[:10]
        checkout = res.get("departure", res.get("check_out", ""))[:10]

        if not checkin or not checkout:
            continue

        # Calculate nights
        try:
            checkin_dt = datetime.strptime(checkin, "%Y-%m-%d").date()
            checkout_dt = datetime.strptime(checkout, "%Y-%m-%d").date()
            nights = (checkout_dt - checkin_dt).days
        except ValueError:
            continue

        if nights < MIN_NIGHTS:
            print(f"  Skip: {nights} nights (< {MIN_NIGHTS})")
            continue

        guest = res.get("guest", {}) or {}
        first_name = guest.get("first_name", "").strip() or "Guest"
        full_name = f"{guest.get('first_name', '')} {guest.get('last_name', '')}".strip() or "Guest"
        res_code = res.get("reservation_code", res.get("id", ""))

        days_until = (checkin_dt - today).days
        day_of_week = checkin_dt.strftime("%A")
        checkin_friendly = friendly_date(checkin)

        # Look up guest email from Guest Contacts
        guest_email = get_guest_email(full_name)

        print(f"  {full_name}: {nights} nights, check-in {checkin} ({days_until} days away)")
        print(f"  Email: {guest_email or 'N/A'}")

        # Day before at 11am PT: initial text to Bryant
        if days_until == 1 and 10 <= current_hour <= 12:
            message = (
                f"Hey Bryant, the Airbnb guest {first_name} checking in on "
                f"{checkin_friendly} is staying >5 nights so we've promised a perk. "
                f"Can you send the $50 Sea Creatures gift card before 4pm check-in "
                f"on {day_of_week}?"
            )
            if guest_email:
                message += f"\nGuest Email: {guest_email}"
            else:
                message += "\nGuest Email: not available yet"

            send_text(BRYANT_PHONE, message)

            if not action_item_exists(res_code):
                create_gift_card_action_item(full_name, checkin, res_code, nights)
                print(f"  Action item created")
            else:
                print(f"  Action item already exists")

        # Day of at 12pm PT: follow-up only if Bryant hasn't replied
        elif days_until == 0 and 11 <= current_hour <= 13:
            has_replied = check_for_reply(BRYANT_PHONE, since_hours=26)

            if not has_replied:
                message = (
                    "Hey Bryant, just confirming you've sent the gift card "
                    "to the guest. Let me know. Thanks!"
                )
                send_text(BRYANT_PHONE, message)
                print(f"  Follow-up sent (no reply detected)")
            else:
                print(f"  Bryant already replied, skipping follow-up")

        else:
            print(f"  No action needed right now ({days_until} days away, {current_hour}:00 PT)")

    print("Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import sys, traceback
        print(f"\nFATAL: {e}")
        traceback.print_exc()
        sys.exit(1)
