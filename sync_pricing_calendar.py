#!/usr/bin/env python3
"""
Sync Pricing Calendar in Notion with Hospitable calendar data.
Runs daily to keep the calendar rolling and accurate.

- Pulls live pricing and availability from Hospitable API
- Updates existing rows where price, status, or min stay changed
- Adds new dates as they roll into the 12-month window
- Archives dates that have passed
"""

import json, urllib.request, urllib.error, time, ssl
from datetime import datetime, timedelta

# --- Config ---
# Load tokens -- env vars first (GitHub Actions), then local files
import os
HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")

if not HOSPITABLE_PAT:
    try:
        with open(os.path.join(os.path.dirname(__file__), '.env')) as f:
            for line in f:
                if line.startswith('HOSPITABLE_PAT='):
                    HOSPITABLE_PAT = line.strip().split('=', 1)[1]
    except FileNotFoundError:
        pass

if not NOTION_TOKEN:
    try:
        with open(os.path.expanduser('~/.claude.json')) as f:
            config = json.load(f)
        headers_str = config['mcpServers']['notionApi']['env']['OPENAPI_MCP_HEADERS']
        NOTION_TOKEN = json.loads(headers_str)['Authorization'].replace('Bearer ', '')
    except (FileNotFoundError, KeyError):
        pass

PRICING_DB = "32750c17-99cc-813d-a837-c30831ba4773"

# Property mapping: Hospitable UUID -> STR Listing Notion ID
# Add more properties here to scale beyond Chad
PROPERTIES = {
    "eefb5918-5149-4b4e-bdd0-277754409cb0": {
        "listing_id": "32050c17-99cc-8188-9bfd-f23a4cc8c028",
        "name": "Chad: European Inspired Phinney Flat"
    }
}

ctx = ssl.create_default_context()
DAYS_SHORT = {'MONDAY': 'Mon', 'TUESDAY': 'Tue', 'WEDNESDAY': 'Wed',
              'THURSDAY': 'Thu', 'FRIDAY': 'Fri', 'SATURDAY': 'Sat', 'SUNDAY': 'Sun'}

# --- API helpers ---

def notion_request(endpoint, data, method="POST"):
    req = urllib.request.Request(
        f"https://api.notion.com/v1{endpoint}",
        data=json.dumps(data).encode() if data else None,
        headers={
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        },
        method=method
    )
    for attempt in range(3):
        try:
            resp = urllib.request.urlopen(req, context=ctx)
            return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print(f"  Rate limited, retrying in 5s...")
                time.sleep(5)
            else:
                body = e.read().decode()[:200]
                print(f"  Notion error {e.code}: {body}")
                return None
    return None

def hospitable_calendar(property_uuid, start_date, end_date):
    """Pull calendar from Hospitable API. Returns list of day objects."""
    url = f"https://public.api.hospitable.com/v2/properties/{property_uuid}/calendar?start_date={start_date}&end_date={end_date}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {HOSPITABLE_PAT}",
        "Accept": "application/json"
    })
    try:
        resp = urllib.request.urlopen(req, context=ctx)
        data = json.loads(resp.read())
        return data.get('data', {}).get('days', [])
    except urllib.error.HTTPError as e:
        print(f"  Hospitable API error {e.code}: {e.read().decode()[:300]}")
        return []
    except Exception as e:
        print(f"  Hospitable API error: {e}")
        return []

def get_existing_rows():
    """Fetch all existing pricing calendar rows from Notion, keyed by date string."""
    rows = {}
    start_cursor = None
    while True:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        resp = notion_request(f"/databases/{PRICING_DB}/query", payload)
        if not resp:
            break
        for page in resp.get('results', []):
            date_prop = page['properties'].get('Date', {}).get('date')
            if date_prop and date_prop.get('start'):
                rows[date_prop['start']] = {
                    'page_id': page['id'],
                    'price': page['properties'].get('Nightly Rate', {}).get('number'),
                    'status': (page['properties'].get('Status', {}).get('select') or {}).get('name', ''),
                    'min_stay': page['properties'].get('Min Stay', {}).get('number'),
                }
        if not resp.get('has_more'):
            break
        start_cursor = resp['next_cursor']
    return rows

def make_title(price, status):
    if status == 'Booked':
        return f"${price} - Booked"
    return f"${price}"

# --- Main sync ---

def sync():
    today = datetime.now().strftime('%Y-%m-%d')
    end_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')

    print(f"=== Pricing Calendar Sync: {today} ===")
    print(f"Window: {today} to {end_date}")
    print(f"Source: Hospitable API\n")

    # Step 1: Get existing Notion rows
    print("Fetching existing Notion rows...")
    existing = get_existing_rows()
    print(f"  Found {len(existing)} existing rows\n")

    # Step 2: Process each property
    created = 0
    updated = 0
    archived = 0
    unchanged = 0

    for prop_uuid, prop_info in PROPERTIES.items():
        print(f"Fetching calendar: {prop_info['name']}...")
        listing_id = prop_info['listing_id']

        # Hospitable API may have limits on date range, so chunk by 3 months
        current = datetime.strptime(today, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        all_days = []

        while current < end:
            chunk_end = min(current + timedelta(days=90), end)
            days = hospitable_calendar(
                prop_uuid,
                current.strftime('%Y-%m-%d'),
                chunk_end.strftime('%Y-%m-%d')
            )
            all_days.extend(days)
            current = chunk_end + timedelta(days=1)
            time.sleep(0.5)  # be nice to the API

        print(f"  Got {len(all_days)} days from Hospitable\n")

        fresh_dates = set()

        for day in all_days:
            date_str = day['date']
            if date_str < today or date_str > end_date:
                continue

            fresh_dates.add(date_str)
            day_name = DAYS_SHORT.get(day.get('day', ''), '')
            available = day.get('status', {}).get('available', True)
            status = 'Available' if available else 'Booked'
            price = day['price']['amount'] // 100  # cents to dollars
            min_stay = day.get('min_stay')
            title = make_title(price, status)

            if date_str in existing:
                row = existing[date_str]
                # Check if anything changed
                if row['price'] == price and row['status'] == status and row['min_stay'] == min_stay:
                    unchanged += 1
                    continue

                # Update existing row
                update_props = {
                    "Name": {"title": [{"text": {"content": title}}]},
                    "Nightly Rate": {"number": price},
                    "Status": {"select": {"name": status}},
                }
                if min_stay:
                    update_props["Min Stay"] = {"number": min_stay}
                notion_request(f"/pages/{row['page_id']}", {"properties": update_props}, method="PATCH")
                updated += 1
                if updated <= 20:
                    print(f"  Updated: {date_str} -> {title} (min {min_stay})")
                time.sleep(0.3)
            else:
                # Create new row
                props = {
                    "Name": {"title": [{"text": {"content": title}}]},
                    "Date": {"date": {"start": date_str}},
                    "Nightly Rate": {"number": price},
                    "Status": {"select": {"name": status}},
                    "Day": {"rich_text": [{"text": {"content": day_name}}]},
                    "Property": {"relation": [{"id": listing_id}]}
                }
                if min_stay:
                    props["Min Stay"] = {"number": min_stay}
                notion_request("/pages", {"parent": {"database_id": PRICING_DB}, "properties": props})
                created += 1
                time.sleep(0.35)

        if created > 0:
            print(f"  Created {created} new rows")
        if updated > 20:
            print(f"  ... and {updated - 20} more updates")

    # Step 3: Archive past dates
    print("\nArchiving past dates...")
    for date_str, row in existing.items():
        if date_str < today:
            notion_request(f"/pages/{row['page_id']}", {"archived": True}, method="PATCH")
            archived += 1
            time.sleep(0.3)

    print(f"\n=== Sync Complete ===")
    print(f"  Created:   {created}")
    print(f"  Updated:   {updated}")
    print(f"  Archived:  {archived}")
    print(f"  Unchanged: {unchanged}")

if __name__ == "__main__":
    try:
        sync()
    except Exception as e:
        import sys
        print(f"\nFATAL: {e}")
        sys.exit(1)
