#!/usr/bin/env python3
"""
One-time backfill for Lock Activity:
1. Populate "Lock Names" column with HA friendly names (from raw entity IDs)
2. Clean up Name (title) and Person fields with readable cleaner names

Safe to re-run — skips rows already updated.
"""

import json, os, re, ssl, sys, urllib.request, urllib.error

LOCK_ACTIVITY_DB_ID = "33450c17-99cc-81e0-b025-f81a69944156"

# Same mappings as sync_lock_history.py
CLEANER_TEAM_MAP = {
    "cleaner: ana": "ana",
    "ana cleanpt7": "ana",
    "ana clean2ho": "ana",
    "ana cleanwrc": "ana",
    "ana cleaner": "ana",
    "ana guayllas": "ana",
    "cleaner: gilda": "gilda",
    "cleaner: gilda team": "gilda",
    "gilda team": "gilda",
    "jiselle j4x1": "jiselle",
    "jiselle j5qi": "jiselle",
    "jay's cleaners": "jiselle",
    "owner: don and kathy": "don and kathy",
}
CLEANER_DISPLAY_NAMES = {
    "ana": "Ana Guayllas",
    "gilda": "Gilda Camargo",
    "jiselle": "Jay's Cleaning",
    "don and kathy": "Don & Kathy",
}


def clean_person_name(raw_name):
    if not raw_name:
        return raw_name
    team_key = CLEANER_TEAM_MAP.get(raw_name.lower().strip())
    if team_key:
        return CLEANER_DISPLAY_NAMES.get(team_key, raw_name)
    return raw_name

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
        props_patch = {}

        # --- Lock Names backfill ---
        existing_names = page["properties"].get("Lock Names", {}).get("rich_text", [])
        if not (existing_names and existing_names[0].get("plain_text", "")):
            rt = page["properties"].get("Lock Entities Used", {}).get("rich_text", [])
            if rt:
                raw_value = rt[0].get("plain_text", "")
                if raw_value:
                    parts = [p.strip() for p in raw_value.split(",")]
                    friendly_parts = [names.get(p, p) for p in parts]
                    friendly_value = ", ".join(friendly_parts)
                    props_patch["Lock Names"] = {
                        "rich_text": [{"text": {"content": friendly_value[:200]}}]
                    }

        # --- Person + Title cleanup ---
        person_rt = page["properties"].get("Person", {}).get("rich_text", [])
        raw_person = person_rt[0]["plain_text"] if person_rt else ""
        clean_person = clean_person_name(raw_person)

        if raw_person and clean_person != raw_person:
            props_patch["Person"] = {
                "rich_text": [{"text": {"content": clean_person[:100]}}]
            }

        # Rebuild title: "Person — Property — Mon D"
        title_rt = page["properties"].get("Name", {}).get("title", [])
        old_title = title_rt[0]["plain_text"] if title_rt else ""
        if old_title and " — " in old_title:
            title_parts = old_title.split(" — ")
            if len(title_parts) == 3:
                # Old format: "RawPerson — Property — YYYY-MM-DD"
                old_person_part, prop_part, date_part = title_parts
                new_person_part = clean_person_name(old_person_part.strip())
                # Convert "2026-03-22" to "Mar 22"
                short_date = date_part.strip()
                try:
                    from datetime import datetime
                    dt = datetime.strptime(short_date, "%Y-%m-%d")
                    short_date = dt.strftime("%b %-d")
                except ValueError:
                    pass
                type_sel = page["properties"].get("Type", {}).get("select")
                entry_type = type_sel["name"] if type_sel else ""
                icon = "\U0001F9F9" if entry_type == "Cleaner" else "\U0001F511"
                # Strip any existing 🏡 prefix from prop_part
                clean_prop = prop_part.strip().lstrip("\U0001F3E1").strip()
                new_title = f"{icon} {new_person_part} — \U0001F3E1 {clean_prop} — {short_date}"
                if new_title != old_title:
                    props_patch["Name"] = {
                        "title": [{"text": {"content": new_title[:100]}}]
                    }

        if not props_patch:
            skipped += 1
            continue

        result = notion_request("PATCH", f"/pages/{page['id']}", {
            "properties": props_patch
        })
        if result:
            new_title = props_patch.get("Name", {}).get("title", [{}])[0].get("text", {}).get("content", old_title)
            changes = ", ".join(props_patch.keys())
            print(f"  Updated ({changes}): {new_title}")
            updated += 1
        else:
            print(f"  FAILED to update page {page['id']}")

    print(f"\nDone. Updated {updated}, skipped {skipped}")


if __name__ == "__main__":
    main()
