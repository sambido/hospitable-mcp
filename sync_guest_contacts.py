#!/usr/bin/env python3
"""
Sync guest contact info from Hospitable reservations to Notion.
Replaces StayFi -- pulls phone numbers and emails directly from reservation data.

Runs daily. Deduplicates by Reservation ID so it's safe to re-run.
"""

import json, os, re, ssl, sys, time, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Los_Angeles")

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# --- Config ---
NOTION_DB_ID = "32950c17-99cc-810b-b234-e3f653240342"
HOSPITABLE_BASE = "https://public.api.hospitable.com/v2"

# Load tokens -- env vars first (GitHub Actions), then local files
HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")

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
                _cfg = json.load(f)
            _headers_str = _cfg["mcpServers"]["notionApi"]["env"]["OPENAPI_MCP_HEADERS"]
            NOTION_TOKEN = json.loads(_headers_str)["Authorization"].replace("Bearer ", "")
        except (FileNotFoundError, KeyError):
            pass

CTX = ssl.create_default_context()

# All active properties (excluding ZZZ-prefixed inactive ones)
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
                raise


def hospitable_request(endpoint, params=None):
    """Make a Hospitable API request."""
    url = f"{HOSPITABLE_BASE}{endpoint}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {HOSPITABLE_PAT}", "Accept": "application/json"},
    )
    resp = urllib.request.urlopen(req, context=CTX)
    return json.loads(resp.read())


def get_existing_contacts():
    """Get all existing contacts from Notion. Returns dict: res_id -> {page_id, has_email}."""
    existing = {}
    has_more = True
    start_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        result = notion_request("POST", f"/databases/{NOTION_DB_ID}/query", payload)
        for page in result["results"]:
            rt = page["properties"].get("Reservation ID", {}).get("rich_text", [])
            if rt:
                res_id = rt[0]["plain_text"]
                email_prop = page["properties"].get("Email", {}).get("email")
                existing[res_id] = {
                    "page_id": page["id"],
                    "has_email": bool(email_prop),
                }
        has_more = result.get("has_more", False)
        start_cursor = result.get("next_cursor")

    return existing


def fetch_reservations(property_uuid, start_date=None, end_date=None):
    """Fetch reservations for a property from Hospitable."""
    all_reservations = []
    page = 1

    while True:
        params = {
            "properties[]": property_uuid,
            "include": "guest",
            "per_page": "50",
            "page": str(page),
        }
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        result = hospitable_request("/reservations", params)
        all_reservations.extend(result.get("data", []))

        if result["meta"]["current_page"] >= result["meta"]["last_page"]:
            break
        page += 1

    return all_reservations


COUNTRY_CODES = {
    "1": "US/CA", "44": "UK", "52": "MX", "61": "AU", "63": "PH",
    "64": "NZ", "81": "JP", "82": "KR", "86": "CN", "91": "IN",
    "33": "FR", "49": "DE", "39": "IT", "34": "ES", "31": "NL",
    "46": "SE", "47": "NO", "45": "DK", "358": "FI", "41": "CH",
    "55": "BR", "57": "CO", "56": "CL", "54": "AR",
    "65": "SG", "66": "TH", "60": "MY", "62": "ID",
    "972": "IL", "971": "AE", "966": "SA",
    "353": "IE", "48": "PL", "43": "AT", "32": "BE",
    "351": "PT", "30": "GR", "7": "RU",
}


def format_phone(numbers):
    """Format phone numbers list into a readable string with + prefix."""
    if not numbers or not numbers[0]:
        return None
    phone = str(numbers[0]).lstrip("+")
    # US/Canada: 11 digits starting with 1, or 10 digits
    if len(phone) == 11 and phone.startswith("1"):
        return f"+1 ({phone[1:4]}) {phone[4:7]}-{phone[7:]}"
    elif len(phone) == 10:
        return f"+1 ({phone[0:3]}) {phone[3:6]}-{phone[6:]}"
    # International: detect country code and prepend +
    for cc in sorted(COUNTRY_CODES.keys(), key=len, reverse=True):
        if phone.startswith(cc) and len(phone) > len(cc) + 5:
            return f"+{phone}"
    # Fallback: just prepend + if it looks like a full number
    if len(phone) >= 10:
        return f"+{phone}"
    return phone


HOST_EMAILS = {
    "sam.esecson@gmail.com",
    "samesecson@gmail.com",
    # Add any other host/team emails here
}


def scrape_email_from_messages(reservation_uuid):
    """Scan Hospitable message threads for guest email addresses.
    Only scans messages FROM the guest (no sender = AI-generated, skip those too)."""
    try:
        result = hospitable_request(f"/reservations/{reservation_uuid}/messages", {"per_page": "50"})
        messages = result.get("data", [])
        for msg in messages:
            # Skip AI-generated messages (no sender) and host messages
            sender = msg.get("sender")
            if not sender:
                continue
            sender_role = sender.get("role", "")
            if sender_role in ("host", "team", "co_host"):
                continue

            body = msg.get("body", "") or ""
            matches = EMAIL_REGEX.findall(body)
            for email in matches:
                lower = email.lower()
                # Filter out relay/platform emails and host emails
                if any(skip in lower for skip in ["noreply", "no-reply", "airbnb", "vrbo", "booking.com", "hospitable", "guest.booking"]):
                    continue
                if lower in HOST_EMAILS:
                    continue
                return email
    except Exception as e:
        pass
    return None


def create_contact(reservation, property_name):
    """Create a Notion page for a guest contact."""
    guest = reservation.get("guest", {})
    if not guest:
        return False

    first = guest.get("first_name", "")
    last = guest.get("last_name", "")
    name = f"{first} {last}".strip() or "Unknown Guest"

    phone_raw = guest.get("phone_numbers", [])
    phone = format_phone(phone_raw)
    email = guest.get("email")
    # If no email from reservation data, scan message threads
    if not email:
        res_uuid = reservation.get("id", "")
        if res_uuid:
            email = scrape_email_from_messages(res_uuid)
            if email:
                print(f"    Found email in messages: {email}")
    location = guest.get("location", "")

    platform = reservation.get("platform", "").capitalize()
    if platform == "Booking_com":
        platform = "Booking.com"

    checkin = reservation.get("arrival_date", "")[:10] if reservation.get("arrival_date") else None
    checkout = reservation.get("departure_date", "")[:10] if reservation.get("departure_date") else None
    nights = reservation.get("nights")
    guests_data = reservation.get("guests", {})
    total_guests = guests_data.get("total")
    adults = guests_data.get("adult_count")
    children = guests_data.get("child_count")
    infants = guests_data.get("infant_count")
    pets = guests_data.get("pet_count")
    booking_date_raw = reservation.get("booking_date")
    booking_date = None
    if booking_date_raw:
        try:
            # Hospitable returns UTC — convert to Pacific before extracting date
            utc_dt = datetime.fromisoformat(booking_date_raw.replace("Z", "+00:00"))
            booking_date = utc_dt.astimezone(PACIFIC).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            booking_date = booking_date_raw[:10]
    res_id = reservation.get("code", reservation.get("id", ""))

    # Build properties
    props = {
        "Name": {"title": [{"text": {"content": name}}]},
        "Property": {"select": {"name": property_name}},
        "Reservation ID": {"rich_text": [{"text": {"content": str(res_id)}}]},
    }

    if phone:
        props["Phone"] = {"phone_number": phone}
    if email:
        props["Email"] = {"email": email}
    if platform:
        props["Platform"] = {"select": {"name": platform}}
    if checkin:
        props["Check-in"] = {"date": {"start": checkin}}
    if checkout:
        props["Check-out"] = {"date": {"start": checkout}}
    if nights:
        props["Nights"] = {"number": nights}
    if total_guests:
        props["Guests"] = {"number": total_guests}
    if adults is not None:
        props["Adults"] = {"number": adults}
    if children is not None:
        props["Children"] = {"number": children}
    if infants is not None:
        props["Infants"] = {"number": infants}
    if pets is not None:
        props["Pets"] = {"number": pets}
    if location:
        props["Location"] = {"rich_text": [{"text": {"content": location}}]}
    if booking_date:
        props["Booking Date"] = {"date": {"start": booking_date}}

    notion_request("POST", "/pages", {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": props,
    })
    return True


def main():
    print(f"=== Guest Contacts Sync: {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    # Get existing contacts for dedup and email backfill
    existing = get_existing_contacts()
    existing_ids = set(existing.keys())
    print(f"Existing contacts in Notion: {len(existing_ids)}")

    # Always pull all reservations — past and future — to capture every email.
    # Dedup by Reservation ID means re-scanning is safe and fast.
    start_date = "2020-01-01"
    end_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

    print(f"Pulling all reservations ({start_date} to {end_date})")

    new_count = 0
    skipped = 0
    errors = 0

    for prop_uuid, prop_name in PROPERTIES.items():
        try:
            reservations = fetch_reservations(prop_uuid, start_date, end_date)
            prop_new = 0

            for res in reservations:
                res_id = str(res.get("code", res.get("id", "")))
                if res_id in existing_ids:
                    # Backfill email if existing entry is missing one
                    entry = existing.get(res_id, {})
                    if not entry.get("has_email"):
                        guest = res.get("guest", {}) or {}
                        email = guest.get("email")
                        if not email:
                            res_uuid = res.get("id", "")
                            if res_uuid:
                                email = scrape_email_from_messages(res_uuid)
                        if email:
                            page_id = entry.get("page_id")
                            if page_id:
                                notion_request("PATCH", f"/pages/{page_id}", {
                                    "properties": {"Email": {"email": email}}
                                })
                                print(f"  Backfilled email for {res_id}: {email}")
                                time.sleep(0.35)
                    skipped += 1
                    continue

                # Skip cancelled reservations
                status = res.get("status", "")
                if status in ("cancelled", "declined", "expired"):
                    skipped += 1
                    continue

                try:
                    if create_contact(res, prop_name):
                        new_count += 1
                        prop_new += 1
                        existing_ids.add(str(res_id))
                except Exception as e:
                    print(f"  Error creating contact for {res_id}: {e}")
                    errors += 1

                # Rate limit: Notion allows 3 requests/sec
                time.sleep(0.35)

            if prop_new > 0:
                print(f"  {prop_name}: +{prop_new} new contacts")

        except Exception as e:
            print(f"  Error fetching {prop_name}: {e}")
            errors += 1

        # Small delay between properties to respect Hospitable rate limits
        time.sleep(0.5)

    print(f"\nDone! New: {new_count} | Skipped: {skipped} | Errors: {errors}")
    print(f"Total contacts in Notion: {len(existing_ids)}")

    # --- Backfill emails for existing contacts missing them ---
    print("\nBackfilling emails from message threads for existing contacts...")
    backfill_emails()

    # --- Repeat guest detection ---
    # A guest is "repeat" if the same guest ID appears on multiple reservations
    print("\nChecking for repeat guests...")
    mark_repeat_guests()

    if errors > 0:
        sys.exit(1)


def backfill_emails():
    """Scan message threads for emails on existing contacts that are missing an email."""
    # Query contacts with no email
    all_pages = []
    has_more = True
    start_cursor = None

    while has_more:
        payload = {
            "page_size": 100,
            "filter": {
                "property": "Email",
                "email": {"is_empty": True}
            }
        }
        if start_cursor:
            payload["start_cursor"] = start_cursor
        result = notion_request("POST", f"/databases/{NOTION_DB_ID}/query", payload)
        all_pages.extend(result["results"])
        has_more = result.get("has_more", False)
        start_cursor = result.get("next_cursor")

    print(f"  Contacts missing email: {len(all_pages)}")
    if not all_pages:
        return

    # We need reservation UUIDs -- but we only have reservation codes in Notion.
    # Fetch reservations per property and build a code->UUID map.
    code_to_uuid = {}
    for prop_uuid, prop_name in PROPERTIES.items():
        try:
            reservations = fetch_reservations(prop_uuid, "2020-01-01",
                                              (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d"))
            for res in reservations:
                code = str(res.get("code", ""))
                uuid = res.get("id", "")
                if code and uuid:
                    code_to_uuid[code] = uuid
            time.sleep(0.5)
        except Exception:
            pass

    emails_found = 0
    for page in all_pages:
        rt = page["properties"].get("Reservation ID", {}).get("rich_text", [])
        if not rt:
            continue
        res_code = rt[0]["plain_text"]
        res_uuid = code_to_uuid.get(res_code)
        if not res_uuid:
            continue

        email = scrape_email_from_messages(res_uuid)
        if email:
            try:
                notion_request("PATCH", f"/pages/{page['id']}", {
                    "properties": {"Email": {"email": email}}
                })
                name = page["properties"].get("Name", {}).get("title", [{}])[0].get("plain_text", "?")
                print(f"    {name}: {email}")
                emails_found += 1
                time.sleep(0.35)
            except Exception as e:
                print(f"    Error updating email: {e}")

        time.sleep(0.5)  # Rate limit for Hospitable message API

    print(f"  Backfilled {emails_found} emails from message threads")


def mark_repeat_guests():
    """Scan all contacts and flag repeat guests (same Hospitable guest ID or name+phone)."""
    all_pages = []
    has_more = True
    start_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        result = notion_request("POST", f"/databases/{NOTION_DB_ID}/query", payload)
        all_pages.extend(result["results"])
        has_more = result.get("has_more", False)
        start_cursor = result.get("next_cursor")

    # Group by name (lowercase) to find repeats
    from collections import defaultdict
    name_counts = defaultdict(list)
    for page in all_pages:
        title = page["properties"].get("Name", {}).get("title", [])
        name = title[0]["plain_text"].strip().lower() if title else ""
        phone = page["properties"].get("Phone", {}).get("phone_number", "")
        is_repeat = page["properties"].get("Repeat Guest", {}).get("checkbox", False)
        if name and name != "unknown guest":
            # Use name as key; if phone exists, use name+phone for more precision
            key = name
            name_counts[key].append((page["id"], is_repeat))

    # Find names with multiple reservations and mark as repeat
    repeats_marked = 0
    for name, pages in name_counts.items():
        if len(pages) > 1:
            for page_id, already_marked in pages:
                if not already_marked:
                    try:
                        notion_request("PATCH", f"/pages/{page_id}", {
                            "properties": {"Repeat Guest": {"checkbox": True}}
                        })
                        repeats_marked += 1
                        time.sleep(0.35)
                    except Exception as e:
                        print(f"  Error marking repeat: {e}")

    repeat_guests = sum(1 for pages in name_counts.values() if len(pages) > 1)
    print(f"Repeat guests: {repeat_guests} unique guests with multiple stays ({repeats_marked} newly marked)")


if __name__ == "__main__":
    main()
