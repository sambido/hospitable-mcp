#!/usr/bin/env python3
"""
Sync Turnover Schedule from Hospitable to Notion.

Pulls reservations for the next 3 days, scans guest message threads for
check-in/checkout times, uses Claude to extract structured times, and writes
to the Turnover Schedule Notion database.

Runs daily (6am and noon PT). Safe to re-run — deduplicates by reservation ID.
"""

import json, os, ssl, sys, time, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Los_Angeles")

# --- Config ---
TURNOVER_DB_ID = "33150c17-99cc-8195-acc9-c17252ea68a3"
STR_LISTINGS_DB_ID = "1eb50c17-99cc-8091-a8ea-e0ba6ec649ff"
HOSPITABLE_BASE = "https://public.api.hospitable.com/v2"
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# How many days ahead to look for turnovers
LOOKAHEAD_DAYS = 3

# Load tokens -- env vars first (GitHub Actions), then local files
HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

if not HOSPITABLE_PAT or not NOTION_TOKEN or not ANTHROPIC_API_KEY:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)
        HOSPITABLE_PAT = HOSPITABLE_PAT or os.environ.get("HOSPITABLE_PAT", "")
        ANTHROPIC_API_KEY = ANTHROPIC_API_KEY or os.environ.get("ANTHROPIC_API_KEY", "")
    except ImportError:
        pass
    if not NOTION_TOKEN:
        try:
            with open(os.path.expanduser("~/.claude.json")) as f:
                _cfg = json.load(f)
            _headers_str = _cfg["mcpServers"]["notionApi"]["env"]["OPENAPI_MCP_HEADERS"]
            NOTION_TOKEN = json.loads(_headers_str)["Authorization"].replace("Bearer ", "")
        except (FileNotFoundError, KeyError):
            pass

CTX = ssl.create_default_context()

# All active properties — Hospitable UUID to friendly name
PROPERTIES = {
    "92a1c198-4d3e-4d1b-a5f8-e90f98f1c49c": "65th",
    "13a74151-c6bc-434b-8de1-549f048d77c7": "Gunny",
    "f1970a87-2c41-4cd8-b222-329980b45a78": "Mary Anne",
    "c50f431b-1d44-40fd-8788-92708710a1cc": "Andy",
    "f3fd4981-3f21-4c5a-8888-ba259834ddb5": "8th",
    "bd0528ad-c1cb-4035-821a-fb1199dfacaa": "Chris",
    "d708140c-4ba0-4673-ba44-0b11d4f97181": "Lia",
    "c80e149c-0ae3-4cf1-965b-5fd12e97f7f6": "Sophia",
    "bef6a386-1446-4c09-a7db-757824cd6d35": "Eve",
    "ab7b6a1b-b731-4046-8406-654a3b62b2cb": "Assim",
    "10bd7b2b-e250-416f-b45f-a1a4d0e92e3c": "Susan",
    "eefb5918-5149-4b4e-bdd0-277754409cb0": "Chad",
    "56ea4fe3-3445-4a6b-962f-a02cbbd2869b": "Matthew",
    "123ee545-ddf9-4e25-b6d0-e597afc5612b": "Jeremy",
    "5cf63104-6ae7-40b2-aa7d-c10d18822ccd": "Don and Kathy",
    "9bfda321-b0f0-4c4e-8f03-eeb86ef3c87f": "Sundee",
    "4dbf5125-6efe-4097-90f6-3fab87a911d2": "Bridget",
    "14912b54-f5e0-47ac-a8c2-1e1d9e17bbd6": "Adam",
    "8423a091-1513-4d98-9e68-c6c3888b1f9e": "Michael",
    "a8cd20bc-16f9-44d0-8c3f-12bea51720cb": "Nordic Loft",
    "c84923ff-a37b-4463-93d6-d192de05be78": "Danial",
    "df375ad6-b2e8-43de-a7f2-45d658864736": "Miller Bay",
    "d92cdc13-8bd9-4803-a277-55f8ba36bd29": "Lower Unit",
}

# Hospitable UUID -> Notion STR Listings page ID (for relation field)
PROPERTY_NOTION_IDS = {
    "92a1c198-4d3e-4d1b-a5f8-e90f98f1c49c": "32050c17-99cc-81d1-8434-e4366b86acc0",  # 65th
    "13a74151-c6bc-434b-8de1-549f048d77c7": "32050c17-99cc-819a-8271-e687bb4f6e62",  # Gunny
    "f1970a87-2c41-4cd8-b222-329980b45a78": "32050c17-99cc-81b7-82f7-ed78866ed7f8",  # Mary Anne
    "c50f431b-1d44-40fd-8788-92708710a1cc": "32050c17-99cc-81b0-912f-c90506efd195",  # Andy
    "f3fd4981-3f21-4c5a-8888-ba259834ddb5": "1eb50c17-99cc-802c-893d-c1f7599e67ce",  # 8th
    "bd0528ad-c1cb-4035-821a-fb1199dfacaa": "1eb50c17-99cc-8056-8a51-cc3c3e3acc73",  # Chris
    "d708140c-4ba0-4673-ba44-0b11d4f97181": "32050c17-99cc-8106-9965-d054efc61dbc",  # Lia
    "c80e149c-0ae3-4cf1-965b-5fd12e97f7f6": "32050c17-99cc-8176-bc72-c9721385f44c",  # Sophia
    "bef6a386-1446-4c09-a7db-757824cd6d35": "32050c17-99cc-81b9-bef1-c14437a90c7f",  # Eve
    "ab7b6a1b-b731-4046-8406-654a3b62b2cb": "1eb50c17-99cc-8000-8105-d27281de47a4",  # Assim
    "10bd7b2b-e250-416f-b45f-a1a4d0e92e3c": "32050c17-99cc-8191-882d-e32da5c56759",  # Susan
    "eefb5918-5149-4b4e-bdd0-277754409cb0": "32050c17-99cc-8188-9bfd-f23a4cc8c028",  # Chad
    "56ea4fe3-3445-4a6b-962f-a02cbbd2869b": "32050c17-99cc-81cd-af07-fc5780b95a81",  # Matthew
    "123ee545-ddf9-4e25-b6d0-e597afc5612b": "32050c17-99cc-8177-8f32-eed75d037b43",  # Jeremy
    "5cf63104-6ae7-40b2-aa7d-c10d18822ccd": "1eb50c17-99cc-8020-a42f-ea82500a4099",  # Don and Kathy
    "9bfda321-b0f0-4c4e-8f03-eeb86ef3c87f": "1d550c17-99cc-80e2-9c4c-d32b89ccd7f2",  # Sundee
    "4dbf5125-6efe-4097-90f6-3fab87a911d2": "32050c17-99cc-8138-8e44-d992e3009dd7",  # Bridget
    "14912b54-f5e0-47ac-a8c2-1e1d9e17bbd6": "32050c17-99cc-816a-96da-d923599db53e",  # Adam
    "8423a091-1513-4d98-9e68-c6c3888b1f9e": "32050c17-99cc-81cc-9972-d8677481a3ee",  # Michael
    "a8cd20bc-16f9-44d0-8c3f-12bea51720cb": "31f50c17-99cc-8110-9d0f-fe7c5b2ae44b",  # Nordic Loft
    "c84923ff-a37b-4463-93d6-d192de05be78": "32050c17-99cc-81fe-b497-eb7417ca5849",  # Danial
    "df375ad6-b2e8-43de-a7f2-45d658864736": "32050c17-99cc-8196-b3d1-c41c2355904d",  # Miller Bay
    "d92cdc13-8bd9-4803-a277-55f8ba36bd29": "32050c17-99cc-811a-8992-ef25a487d6d1",  # Lower Unit
}


# --------------------------------------------------------------------------- #
# API helpers
# --------------------------------------------------------------------------- #
def notion_request(method, endpoint, data=None, retries=3):
    """Make a Notion API request with retry on transient errors."""
    url = f"https://api.notion.com/v1{endpoint}"
    body = json.dumps(data).encode() if data else None
    for attempt in range(retries):
        req = urllib.request.Request(
            url, data=body, method=method,
            headers={
                "Authorization": f"Bearer {NOTION_TOKEN}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28",
            },
        )
        try:
            resp = urllib.request.urlopen(req, context=CTX)
            return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 502, 503) and attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  Notion {e.code}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Notion error {e.code}: {e.read().decode()[:200]}")
                return None
    return None


def hospitable_get(endpoint, params=None):
    """Make a Hospitable API GET request."""
    url = f"{HOSPITABLE_BASE}{endpoint}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {HOSPITABLE_PAT}",
        "Accept": "application/json",
    })
    resp = urllib.request.urlopen(req, context=CTX)
    return json.loads(resp.read())


def claude_extract_times(messages_text, checkout_date=""):
    """Use Claude to extract check-in/checkout times from guest messages."""
    if not messages_text.strip():
        return None

    system_prompt = """You extract check-in and checkout times from short-term rental guest messages.

Given guest message text, extract:
- checkout_time: when the guest plans to leave / check out
- checkin_time: when the guest plans to arrive / check in
- early_checkin_requested: true if the guest asked about or requested early check-in, false otherwise
- late_checkout_requested: true if the guest asked about or requested late checkout, false otherwise

Rules:
- Return ONLY a raw JSON object: {"checkout_time": "...", "checkin_time": "...", "early_checkin_requested": false, "late_checkout_requested": false, "flight_time": "...", "checkout_is_flight_estimate": false}
- No markdown, no code blocks, no explanation. Just the JSON object.
- Use ONLY simple time format: "3pm", "10:30am", "noon", "early morning". No dates, no commas, no day names.
- Use null for any time not mentioned.
- Only extract times the guest EXPLICITLY states about their own plans.
- "Can I check in early?" without a specific time = null
- "Arriving around 3" = checkin_time: "3pm"
- "We'll be out by 9" = checkout_time: "9am"
- FLIGHT TIME RULE: If the guest mentions a flight departure time but does NOT give an explicit checkout time, set checkout_time to 2 hours before the flight time, set flight_time to the stated flight time, and set checkout_is_flight_estimate to true. Example: "Our flight is at 2pm" = checkout_time: "12pm", flight_time: "2pm", checkout_is_flight_estimate: true. But if they say BOTH "we'll leave by 10am" AND "flight at 2pm", use the explicit time: checkout_time: "10am", flight_time: "2pm", checkout_is_flight_estimate: false.
- "Our flight is at 11am so we'll leave a few hours before" = checkout_time: "9am", flight_time: "11am", checkout_is_flight_estimate: true
- Checkout day PM times: The guest's checkout date will be provided. If a guest says a PM time and it clearly refers to the evening BEFORE checkout day (leaving early), that PM is real. But if a PM time refers to checkout day itself, it's a typo — correct to AM. Example: checkout is March 30, guest says "out by 10pm Sunday" (checkout day) = "10am". But "leaving Saturday evening around 8pm" (night before) = "8pm".
- Check-in times are ALWAYS in the afternoon or evening (PM). If a guest writes an early morning check-in time with "am", correct it to PM.
- Ignore any times mentioned in messages from the host or automated system — only guest-stated times count."""

    user_content = messages_text
    if checkout_date:
        user_content = f"CHECKOUT DATE: {checkout_date}\n\nGUEST MESSAGES:\n{messages_text}"

    body = json.dumps({
        "model": CLAUDE_MODEL,
        "max_tokens": 256,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_content}],
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, context=CTX)
        result = json.loads(resp.read())
        text = result["content"][0]["text"]
        # Strip markdown code blocks if present
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        # Find JSON object in response
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            parsed = json.loads(text[start:end])
            # Guardrail: checkout times like 9pm, 10pm, 11pm are always
            # typos (should be AM). Legit evening departures are earlier
            # (6pm, 7pm, 8pm = guest leaving night before).
            co = parsed.get("checkout_time") or ""
            co_lower = co.lower().strip()
            if co_lower in ("9pm", "10pm", "11pm", "9:00pm", "9:30pm",
                            "10:00pm", "10:30pm", "11:00pm", "11:30pm"):
                parsed["checkout_time"] = co_lower.replace("pm", "am")
            return parsed
    except Exception as e:
        print(f"  Claude API error: {e}")
    return None


# --------------------------------------------------------------------------- #
# Hospitable data fetching
# --------------------------------------------------------------------------- #
def fetch_reservations(property_uuid, start_date, end_date):
    """Fetch reservations for a property within a date range."""
    all_reservations = []
    page = 1
    while True:
        params = {
            "properties[]": property_uuid,
            "include": "guest",
            "per_page": "50",
            "page": str(page),
            "start_date": start_date,
            "end_date": end_date,
        }
        result = hospitable_get("/reservations", params)
        all_reservations.extend(result.get("data", []))
        if result["meta"]["current_page"] >= result["meta"]["last_page"]:
            break
        page += 1
    return all_reservations


def fetch_messages(reservation_uuid):
    """Fetch message thread for a reservation."""
    try:
        result = hospitable_get(
            f"/reservations/{reservation_uuid}/messages",
            {"per_page": "50"},
        )
        return result.get("data", [])
    except Exception as e:
        print(f"  Error fetching messages for {reservation_uuid}: {e}")
        return []


def extract_guest_replies(messages):
    """Collect only guest-sent messages (not host, not automated).
    Returns concatenated text of guest replies only."""
    guest_texts = []
    for msg in messages:
        sender = msg.get("sender")
        if not sender:
            continue  # AI-generated / automated message, skip
        role = sender.get("role", "")
        if role in ("host", "team", "co_host"):
            continue  # Host or team message, skip

        body = msg.get("body", "") or ""
        if not body.strip():
            continue

        guest_texts.append(body)

    return "\n---\n".join(guest_texts)


# --------------------------------------------------------------------------- #
# Notion operations
# --------------------------------------------------------------------------- #
def get_existing_entries():
    """Get existing turnover entries, keyed by Reservation ID."""
    existing = {}
    has_more = True
    start_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        result = notion_request("POST", f"/databases/{TURNOVER_DB_ID}/query", payload)
        if not result:
            break
        for page in result.get("results", []):
            rt = page["properties"].get("Reservation ID", {}).get("rich_text", [])
            if rt:
                res_id = rt[0]["plain_text"]
                existing[res_id] = page["id"]
        has_more = result.get("has_more", False)
        start_cursor = result.get("next_cursor")

    return existing


def archive_old_entries(existing_entries, today_str):
    """Archive entries with checkout dates in the past."""
    payload = {
        "filter": {
            "property": "Checkout Date",
            "date": {"before": today_str},
        }
    }
    result = notion_request("POST", f"/databases/{TURNOVER_DB_ID}/query", payload)
    if not result:
        return 0
    archived = 0
    for page in result.get("results", []):
        if not page.get("archived", False):
            notion_request("PATCH", f"/pages/{page['id']}", {"archived": True})
            archived += 1
    return archived


def upsert_turnover(res_id, page_id, props):
    """Create or update a turnover entry."""
    if page_id:
        # Update existing
        notion_request("PATCH", f"/pages/{page_id}", {"properties": props})
        return "updated"
    else:
        # Create new
        payload = {
            "parent": {"database_id": TURNOVER_DB_ID},
            "properties": props,
        }
        notion_request("POST", "/pages", payload)
        return "created"


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    if not HOSPITABLE_PAT:
        print("ERROR: HOSPITABLE_PAT not set")
        sys.exit(1)
    if not NOTION_TOKEN:
        print("ERROR: NOTION_TOKEN not set")
        sys.exit(1)
    if not ANTHROPIC_API_KEY:
        print("WARNING: ANTHROPIC_API_KEY not set — will use 'Default' for all times")

    now = datetime.now(PACIFIC)
    today = now.strftime("%Y-%m-%d")
    end_date = (now + timedelta(days=LOOKAHEAD_DAYS)).strftime("%Y-%m-%d")

    print(f"Turnover Schedule Sync — {today} to {end_date}")
    print(f"Properties: {len(PROPERTIES)}")

    # Build cleaning team lookup from STR Listings
    cleaning_teams = {}  # Hospitable UUID -> cleaning team name
    print("Loading cleaning teams from STR Listings...")
    has_more = True
    cursor = None
    while has_more:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        result = notion_request("POST", f"/databases/{STR_LISTINGS_DB_ID}/query", payload)
        if not result:
            break
        for page in result.get("results", []):
            page_id = page["id"]
            ct = page["properties"].get("Cleaning Team", {}).get("multi_select", [])
            if ct:
                team_name = ct[0]["name"]  # Use first team if multiple
                # Reverse lookup: find Hospitable UUID for this Notion page ID
                for h_uuid, n_id in PROPERTY_NOTION_IDS.items():
                    if n_id == page_id:
                        cleaning_teams[h_uuid] = team_name
                        break
        has_more = result.get("has_more", False)
        cursor = result.get("next_cursor")
    print(f"  Loaded cleaning teams for {len(cleaning_teams)} properties")

    # Get existing Notion entries for dedup
    existing = get_existing_entries()
    print(f"Existing entries in Notion: {len(existing)}")

    # Archive old entries
    archived = archive_old_entries(existing, today)
    if archived:
        print(f"Archived {archived} past entries")

    # For each property, find reservations with checkouts in our window
    created = 0
    updated = 0

    for prop_uuid, prop_name in PROPERTIES.items():
        print(f"\n--- {prop_name} ---")

        # Hospitable start_date/end_date filter by ARRIVAL date, not departure.
        # To catch all departures in our window, go back far enough to cover
        # the longest possible stay (30 days covers any reasonable STR booking).
        fetch_start = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        fetch_end = (now + timedelta(days=LOOKAHEAD_DAYS + 7)).strftime("%Y-%m-%d")

        try:
            reservations = fetch_reservations(prop_uuid, fetch_start, fetch_end)
        except Exception as e:
            print(f"  Error fetching reservations: {e}")
            continue

        if not reservations:
            print("  No reservations in window")
            continue

        # Sort by arrival date to find consecutive pairs
        reservations.sort(key=lambda r: r.get("arrival_date", ""))

        # Find turnovers: checkout in our window OR check-in in our window
        # A turnover = a departure that needs cleaning
        turnovers_seen = set()

        for i, res in enumerate(reservations):
            departure = res.get("departure_date", "")[:10]
            arrival = res.get("arrival_date", "")[:10]
            res_uuid = res.get("id", "")
            res_code = res.get("code", res_uuid)

            # Include if checkout is in our window
            if not (today <= departure <= end_date):
                continue

            # Skip duplicates
            if res_code in turnovers_seen:
                continue
            turnovers_seen.add(res_code)

            guest = res.get("guest", {}) or {}
            guest_name = f"{guest.get('first_name', '')} {guest.get('last_name', '')}".strip() or "Unknown"

            print(f"  Checkout: {departure} — {guest_name}")

            # Scan messages for checkout time
            checkout_time = None
            checkin_time = None
            late_checkout = False
            early_checkin = False
            time_source = "Default"
            notes = ""

            if ANTHROPIC_API_KEY:
                messages = fetch_messages(res_uuid)
                guest_text = extract_guest_replies(messages)
                if guest_text:
                    times = claude_extract_times(guest_text, checkout_date=departure)
                    if times:
                        checkout_time = times.get("checkout_time")
                        late_checkout = times.get("late_checkout_requested", False)
                        flight_time = times.get("flight_time")
                        is_flight_estimate = times.get("checkout_is_flight_estimate", False)
                        if checkout_time:
                            if is_flight_estimate:
                                time_source = "Flight estimate"
                                notes = f"Flight at {flight_time}" if flight_time else ""
                                print(f"    Checkout time: {checkout_time} (estimated from flight at {flight_time})")
                            else:
                                time_source = "Guest message"
                                print(f"    Checkout time: {checkout_time}")
                        if late_checkout:
                            print(f"    Late checkout requested")

            # Check for same-day or next-day check-in (next reservation)
            next_guest_name = None
            next_checkin_date = None

            if i + 1 < len(reservations):
                next_res = reservations[i + 1]
                next_arrival = next_res.get("arrival_date", "")[:10]
                if next_arrival == departure:
                    # Same-day turnover
                    next_guest = next_res.get("guest", {}) or {}
                    next_guest_name = f"{next_guest.get('first_name', '')} {next_guest.get('last_name', '')}".strip() or "Unknown"
                    next_checkin_date = next_arrival
                    print(f"    Same-day check-in: {next_guest_name}")

                    # Scan next guest's messages for arrival time
                    if ANTHROPIC_API_KEY:
                        next_uuid = next_res.get("id", "")
                        next_messages = fetch_messages(next_uuid)
                        next_guest_text = extract_guest_replies(next_messages)
                        if next_guest_text:
                            next_times = claude_extract_times(next_guest_text, checkout_date=next_arrival)
                            if next_times:
                                checkin_time = next_times.get("checkin_time")
                                early_checkin = next_times.get("early_checkin_requested", False)
                                if checkin_time:
                                    time_source = "Guest message"
                                    print(f"    Check-in time: {checkin_time}")
                                if early_checkin:
                                    print(f"    Early check-in requested")

            # Build Notion properties
            notion_page_id = PROPERTY_NOTION_IDS.get(prop_uuid)
            props = {
                "Name": {"title": [{"text": {"content": f"{prop_name} — {departure}"}}]},
                "Guest Name": {"rich_text": [{"text": {"content": guest_name}}]},
                "Checkout Date": {"date": {"start": departure}},
                "Checkout Time": {"select": {"name": (checkout_time or "11am (default)").replace(",", "")}},
                "Time Source": {"select": {"name": time_source}},
                "Reservation ID": {"rich_text": [{"text": {"content": str(res_code)}}]},
            }
            if notion_page_id:
                props["Property"] = {"relation": [{"id": notion_page_id}]}
            props["Late Checkout Requested"] = {"select": {"name": "Yes" if late_checkout else "No"}}
            props["Early Check-in Requested"] = {"select": {"name": "Yes" if early_checkin else "No"}}
            team = cleaning_teams.get(prop_uuid)
            if team:
                props["Cleaning Team"] = {"select": {"name": team}}

            if next_checkin_date:
                props["Next Check-in Date"] = {"date": {"start": next_checkin_date}}
            if next_guest_name:
                props["Next Guest Name"] = {"rich_text": [{"text": {"content": next_guest_name}}]}
            if next_checkin_date:
                props["Check-in Time"] = {"select": {"name": (checkin_time or "4pm (default)").replace(",", "")}}
            if notes:
                props["Notes"] = {"rich_text": [{"text": {"content": notes}}]}

            # Upsert
            page_id = existing.get(str(res_code))
            action = upsert_turnover(str(res_code), page_id, props)
            if action == "created":
                created += 1
            else:
                updated += 1

    print(f"\n--- Done ---")
    print(f"Created: {created}, Updated: {updated}")


if __name__ == "__main__":
    main()
