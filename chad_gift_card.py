#!/usr/bin/env python3
"""
Chad Gift Card Automation

For stays 5+ nights at Chad's European Inspired Phinney Flat:
- 3 days before check-in: Text Chad + create Action Item in Notion
- 1 day before check-in: Follow-up reminder text to Chad

Runs daily via GitHub Actions.
"""

import json, os, ssl, urllib.request, urllib.error
from datetime import datetime, timedelta

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
CHAD_PHONE = "+17343201846"
MIN_NIGHTS = 5

# Quo (OpenPhone)
QUO_FROM = "PNI52JLEHJ"  # eSam Automations (206) 350-3726

# Notion
ACTION_ITEMS_DB = "33750c17-99cc-81d2-8fc7-c53c747abbc7"

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
        "Date Received": {"date": {"start": datetime.utcnow().strftime("%Y-%m-%d")}},
        "Due Date": {"date": {"start": checkin_date}},
        "Property": {"relation": [{"id": CHAD_PROPERTY_NOTION_ID}]},
        "Outcome Notes": {"rich_text": [{"text": {"content":
            f"$50 Sea Creatures gift card. Guest checking in {checkin_date}. "
            f"Chad delivers to front door of #302 between 11am-4pm on check-in day. "
            f"Text sent to Chad at {CHAD_PHONE}."
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
# Main
# --------------------------------------------------------------------------- #
def main():
    today = datetime.utcnow().date()
    print(f"=== Chad Gift Card Check: {today} ===")

    # Look at check-ins in the next 4 days (catches both 3-day and 1-day windows)
    start = today.isoformat()
    end = (today + timedelta(days=4)).isoformat()

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
        guest_name = f"{guest.get('first_name', '')} {guest.get('last_name', '')}".strip() or "Guest"
        res_code = res.get("reservation_code", res.get("id", ""))

        days_until = (checkin_dt - today).days
        print(f"  {guest_name}: {nights} nights, check-in {checkin} ({days_until} days away)")

        # 3 days before: initial text + create action item
        if days_until == 3:
            message = (
                f"Hey Chad, {guest_name} checking into #302 on {checkin} "
                f"is staying {nights} nights. Can you get the $50 Sea Creatures "
                f"gift card and deliver between 11am - 4pm on check-in day? "
                f"I'll send another reminder the day prior"
            )
            send_text(CHAD_PHONE, message)

            if not action_item_exists(res_code):
                create_gift_card_action_item(guest_name, checkin, res_code, nights)
                print(f"  Action item created")
            else:
                print(f"  Action item already exists")

        # 1 day before: follow-up reminder
        elif days_until == 1:
            message = (
                f"Reminder: {guest_name} checks in tomorrow at #302. "
                f"$50 Sea Creatures gift card to front door between 11am-4pm. Thanks!"
            )
            send_text(CHAD_PHONE, message)
            print(f"  Follow-up reminder sent")

        else:
            print(f"  No action needed today ({days_until} days away)")

    print("Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import sys, traceback
        print(f"\nFATAL: {e}")
        traceback.print_exc()
        sys.exit(1)
