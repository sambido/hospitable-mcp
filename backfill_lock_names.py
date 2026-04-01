#!/usr/bin/env python3
"""
One-time backfill: replace raw entity IDs in Lock Activity "Lock Entities Used"
with friendly names from Home Assistant.

Reads all Lock Activity rows from Notion, queries HA /api/states for friendly
names, and patches any rows that still contain raw entity IDs (lock.xxx).

Safe to re-run — skips rows that already have friendly names.
"""

import json, os, re, ssl, sys, urllib.request, urllib.error

LOCK_ACTIVITY_DB_ID = "33450c17-99cc-81e0-b025-f81a69944156"

# Load tokens
HA_TOKEN = os.environ.get("HA_TOKEN", "").strip()
HA_URL = os.environ.get("HA_URL", "").strip().rstrip("/")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()

if not HA_TOKEN or not HA_URL:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)
        HA_TOKEN = HA_TOKEN or os.environ.get("HA_TOKEN", "")
        HA_URL = (HA_URL or os.environ.get("HA_URL", "")).rstrip("/")
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

if not HA_TOKEN or not HA_URL or not NOTION_TOKEN:
    print("ERROR: Need HA_TOKEN, HA_URL, and NOTION_TOKEN")
    sys.exit(1)

CTX = ssl.create_default_context()


def ha_get_friendly_names():
    """Query HA /api/states for all lock entities and return {entity_id: friendly_name}."""
    url = f"{HA_URL}/api/states"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "WeatherwoodSync/1.0",
    })
    resp = urllib.request.urlopen(req, context=CTX, timeout=30)
    states = json.loads(resp.read())
    names = {}
    for s in states:
        eid = s.get("entity_id", "")
        if eid.startswith("lock."):
            fname = s.get("attributes", {}).get("friendly_name", "")
            if fname:
                names[eid] = fname
    return names


def notion_request(method, path, payload=None):
    url = f"https://api.notion.com/v1{path}"
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    })
    try:
        resp = urllib.request.urlopen(req, context=CTX, timeout=30)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"  Notion API error {e.code}: {e.read().decode()[:200]}")
        return None


def get_all_lock_activity_pages():
    """Fetch all pages from Lock Activity DB."""
    pages = []
    has_more = True
    start_cursor = None
    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        result = notion_request("POST", f"/databases/{LOCK_ACTIVITY_DB_ID}/query", payload)
        if not result:
            break
        pages.extend(result.get("results", []))
        has_more = result.get("has_more", False)
        start_cursor = result.get("next_cursor")
    return pages


def main():
    print("Fetching friendly names from Home Assistant...")
    names = ha_get_friendly_names()
    print(f"  Found {len(names)} lock entities with friendly names")

    if not names:
        print("ERROR: No friendly names found from HA")
        sys.exit(1)

    print("Fetching all Lock Activity pages from Notion...")
    pages = get_all_lock_activity_pages()
    print(f"  Found {len(pages)} pages")

    updated = 0
    skipped = 0
    for page in pages:
        rt = page["properties"].get("Lock Entities Used", {}).get("rich_text", [])
        if not rt:
            skipped += 1
            continue
        old_value = rt[0].get("plain_text", "")
        if not old_value:
            skipped += 1
            continue

        # Check if any part still looks like a raw entity ID (lock.xxx)
        if not re.search(r"\block\.\w+", old_value):
            skipped += 1
            continue

        # Replace each entity ID with its friendly name
        parts = [p.strip() for p in old_value.split(",")]
        new_parts = []
        for part in parts:
            if part in names:
                new_parts.append(names[part])
            else:
                new_parts.append(part)
        new_value = ", ".join(new_parts)

        if new_value == old_value:
            skipped += 1
            continue

        # Update the page
        result = notion_request("PATCH", f"/pages/{page['id']}", {
            "properties": {
                "Lock Entities Used": {
                    "rich_text": [{"text": {"content": new_value[:200]}}]
                }
            }
        })
        if result:
            title_rt = page["properties"].get("Name", {}).get("title", [])
            title = title_rt[0]["plain_text"] if title_rt else page["id"]
            print(f"  Updated: {title}")
            print(f"    {old_value} -> {new_value}")
            updated += 1
        else:
            print(f"  FAILED to update page {page['id']}")

    print(f"\nDone. Updated {updated}, skipped {skipped}")


if __name__ == "__main__":
    main()
