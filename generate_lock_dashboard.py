#!/usr/bin/env python3
"""
Generate a Lock Activity Dashboard page in Notion.

Reads from the Lock Activity database, computes averages and stats,
writes a formatted summary page under Claude Workshop.

Can be run standalone or called after sync_lock_history.py.
"""

import json, os, ssl, sys, time, urllib.request, urllib.error
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Los_Angeles")

# --- Config ---
LOCK_ACTIVITY_DB_ID = "33450c17-99cc-81e0-b025-f81a69944156"
CLAUDE_WORKSHOP_ID = "32d50c17-99cc-80a9-97cb-d07b2be142c8"
DASHBOARD_TITLE = "Lock Activity Dashboard"

NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
if not NOTION_TOKEN:
    try:
        with open(os.path.expanduser("~/.claude.json")) as f:
            _cfg = json.load(f)
        _headers_str = _cfg["mcpServers"]["notionApi"]["env"]["OPENAPI_MCP_HEADERS"]
        NOTION_TOKEN = json.loads(_headers_str)["Authorization"].replace("Bearer ", "")
    except (FileNotFoundError, KeyError):
        pass

CTX = ssl.create_default_context()

# Consolidate cleaner names to canonical names
CLEANER_NAME_MAP = {
    "cleaner: ana": "Ana",
    "ana cleanpt7": "Ana",
    "ana clean2ho": "Ana",
    "ana cleanwrc": "Ana",
    "ana cleaner": "Ana",
    "ana guayllas": "Ana",
    "cleaner: gilda": "Gilda",
    "cleaner: gilda team": "Gilda",
    "gilda team": "Gilda",
    "jiselle j4x1": "Jiselle",
    "jiselle j5qi": "Jiselle",
    "jay's cleaners": "Jiselle",
    "owner: don and kathy": "Don & Kathy (owner-clean)",
}


def normalize_cleaner(name):
    """Map various cleaner code names to canonical names."""
    return CLEANER_NAME_MAP.get(name.lower().strip(), name)


def fmt_duration(minutes):
    """Format minutes as '1hr, 24min' style."""
    if minutes is None:
        return "N/A"
    minutes = round(minutes)
    if minutes < 60:
        return f"{minutes}min"
    hrs = minutes // 60
    mins = minutes % 60
    if mins == 0:
        return f"{hrs}hr"
    return f"{hrs}hr, {mins}min"


# --------------------------------------------------------------------------- #
# Notion helpers
# --------------------------------------------------------------------------- #
def notion_request(method, endpoint, data=None, retries=3):
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
                time.sleep(wait)
            else:
                print(f"  Notion error {e.code}: {e.read().decode()[:200]}")
                return None
    return None


def query_all(db_id, filter_obj=None):
    """Query all pages from a Notion database."""
    all_pages = []
    has_more = True
    cursor = None
    while has_more:
        payload = {"page_size": 100}
        if filter_obj:
            payload["filter"] = filter_obj
        if cursor:
            payload["start_cursor"] = cursor
        result = notion_request("POST", f"/databases/{db_id}/query", payload)
        if not result:
            break
        all_pages.extend(result.get("results", []))
        has_more = result.get("has_more", False)
        cursor = result.get("next_cursor")
    return all_pages


def text_block(content):
    """Create a paragraph block."""
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{"type": "text", "text": {"content": content}}]
        }
    }


def heading2_block(content):
    return {
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [{"type": "text", "text": {"content": content}}]
        }
    }


def heading3_block(content):
    return {
        "object": "block",
        "type": "heading_3",
        "heading_3": {
            "rich_text": [{"type": "text", "text": {"content": content}}]
        }
    }


def code_block(content):
    """Create a code block for formatted tables."""
    return {
        "object": "block",
        "type": "code",
        "code": {
            "rich_text": [{"type": "text", "text": {"content": content}}],
            "language": "plain text",
        }
    }


def divider_block():
    return {"object": "block", "type": "divider", "divider": {}}


# --------------------------------------------------------------------------- #
# Data extraction
# --------------------------------------------------------------------------- #
def extract_entry(page):
    """Extract relevant fields from a Notion page."""
    props = page["properties"]
    title = props.get("Name", {}).get("title", [])
    name = title[0]["plain_text"] if title else ""
    parts = name.split(" \u2014 ")
    property_name = parts[1] if len(parts) >= 3 else "Unknown"

    person_rt = props.get("Person", {}).get("rich_text", [])
    person = person_rt[0]["plain_text"] if person_rt else "Unknown"

    entry_type = props.get("Type", {}).get("select", {})
    entry_type = entry_type.get("name", "") if entry_type else ""

    return {
        "type": entry_type,
        "property": property_name,
        "person": person,
        "duration": props.get("Duration (min)", {}).get("number"),
        "minutes_after_checkout": props.get("Minutes After Checkout", {}).get("number"),
        "minutes_before_checkin": props.get("Minutes Before Check-in", {}).get("number"),
        "same_day": props.get("Same Day Turnover", {}).get("checkbox", False),
        "late_checkout": props.get("Late Checkout", {}).get("checkbox", False),
        "no_show": props.get("No-Show", {}).get("checkbox", False),
        "event_count": props.get("Event Count", {}).get("number"),
        "first_event": props.get("First Event", {}).get("date"),
    }


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    if not NOTION_TOKEN:
        print("ERROR: NOTION_TOKEN not set")
        sys.exit(1)

    now = datetime.now(PACIFIC)
    print("Generating Lock Activity Dashboard...")

    # Fetch all entries
    all_pages = query_all(LOCK_ACTIVITY_DB_ID)
    print(f"Total entries: {len(all_pages)}")

    entries = [extract_entry(p) for p in all_pages]
    cleaners = [e for e in entries if e["type"] == "Cleaner"]
    guests = [e for e in entries if e["type"] == "Guest"]

    # ------------------------------------------------------------------ #
    # CLEANER STATS
    # ------------------------------------------------------------------ #
    # By property
    by_property = defaultdict(list)
    for c in cleaners:
        if c["duration"] is not None:
            by_property[c["property"]].append(c)

    prop_lines = []
    for prop in sorted(by_property.keys()):
        entries_p = by_property[prop]
        durs = [e["duration"] for e in entries_p]
        afters = [e["minutes_after_checkout"] for e in entries_p if e["minutes_after_checkout"] is not None]
        befores = [e["minutes_before_checkin"] for e in entries_p if e["minutes_before_checkin"] is not None]
        same_days = sum(1 for e in entries_p if e["same_day"])
        avg_dur = sum(durs) / len(durs)
        avg_after = sum(afters) / len(afters) if afters else None
        avg_before = sum(befores) / len(befores) if befores else None
        prop_lines.append(
            f"{prop:22s} {fmt_duration(avg_dur):>12s}   "
            f"{len(durs):2d} cleans   "
            f"starts {fmt_duration(avg_after):>10s} after checkout   "
            f"{same_days} same-day"
        )

    # By crew (consolidated)
    by_crew = defaultdict(list)
    for c in cleaners:
        if c["duration"] is not None:
            canonical = normalize_cleaner(c["person"])
            by_crew[canonical].append(c)

    crew_lines = []
    for crew in sorted(by_crew.keys()):
        entries_c = by_crew[crew]
        durs = [e["duration"] for e in entries_c]
        avg_dur = sum(durs) / len(durs)
        props = sorted(set(e["property"] for e in entries_c))
        crew_lines.append(
            f"{crew:25s} {fmt_duration(avg_dur):>12s}   "
            f"{len(durs):2d} cleans   "
            f"properties: {', '.join(props)}"
        )

    # Longest cleans
    long_cleans = sorted(
        [c for c in cleaners if c["duration"] is not None and c["duration"] > 150],
        key=lambda x: x["duration"], reverse=True
    )[:10]

    long_lines = []
    for c in long_cleans:
        long_lines.append(
            f"{fmt_duration(c['duration']):>12s}   "
            f"{c['property']:20s}   "
            f"{normalize_cleaner(c['person'])}"
        )

    # Tightest turnovers (lowest minutes before check-in)
    tight = sorted(
        [c for c in cleaners if c["minutes_before_checkin"] is not None and c["same_day"]],
        key=lambda x: x["minutes_before_checkin"]
    )[:10]

    tight_lines = []
    for c in tight:
        tight_lines.append(
            f"{fmt_duration(c['minutes_before_checkin']):>12s} buffer   "
            f"{c['property']:20s}   "
            f"clean took {fmt_duration(c['duration'])}"
        )

    # ------------------------------------------------------------------ #
    # GUEST STATS
    # ------------------------------------------------------------------ #
    late_checkouts = [g for g in guests if g["late_checkout"]]
    no_shows = [g for g in guests if g["no_show"]]
    guests_with_events = [g for g in guests if g["event_count"] and g["event_count"] > 0]

    # Average events per stay by property
    guest_by_prop = defaultdict(list)
    for g in guests_with_events:
        guest_by_prop[g["property"]].append(g)

    guest_prop_lines = []
    for prop in sorted(guest_by_prop.keys()):
        entries_g = guest_by_prop[prop]
        evts = [e["event_count"] for e in entries_g]
        avg_evts = sum(evts) / len(evts)
        lates = sum(1 for e in entries_g if e["late_checkout"])
        guest_prop_lines.append(
            f"{prop:22s} {avg_evts:5.1f} avg events   "
            f"{len(entries_g):2d} stays   "
            f"{lates} late checkouts"
        )

    # ------------------------------------------------------------------ #
    # BUILD DASHBOARD PAGE
    # ------------------------------------------------------------------ #
    blocks = [
        text_block(f"Auto-generated {now.strftime('%B %d, %Y at %I:%M %p PT')}. "
                   f"Based on {len(cleaners)} cleaner sessions and {len(guests)} guest stays."),
        divider_block(),

        heading2_block("Cleaning Averages by Property"),
        code_block(
            f"{'Property':22s} {'Avg Duration':>12s}   {'Count':>8s}   {'Avg Start':>28s}   {'Same-day':>8s}\n"
            + "-" * 90 + "\n"
            + "\n".join(prop_lines)
        ),

        heading2_block("Cleaning Averages by Crew"),
        code_block(
            f"{'Crew':25s} {'Avg Duration':>12s}   {'Count':>8s}   {'Properties'}\n"
            + "-" * 90 + "\n"
            + "\n".join(crew_lines)
        ),

        heading2_block("Longest Cleaning Sessions"),
        text_block("Cleans over 2.5 hours. May indicate return visits (laundry, inspection) within the 90-min session gap."),
        code_block("\n".join(long_lines) if long_lines else "No cleans over 2.5 hours"),

        heading2_block("Tightest Same-Day Turnovers"),
        text_block("Shortest buffer between cleaner finishing and next guest check-in."),
        code_block("\n".join(tight_lines) if tight_lines else "No same-day turnovers found"),

        divider_block(),

        heading2_block("Guest Activity by Property"),
        code_block(
            f"{'Property':22s} {'Avg Events':>12s}   {'Stays':>7s}   {'Late Checkouts'}\n"
            + "-" * 70 + "\n"
            + "\n".join(guest_prop_lines)
        ),

        heading2_block("Summary Stats"),
        text_block(
            f"Total cleaner sessions: {len(cleaners)}\n"
            f"Total guest stays tracked: {len(guests)}\n"
            f"Late checkouts: {len(late_checkouts)}\n"
            f"No-shows (no lock events): {len(no_shows)}\n"
            f"Same-day turnovers: {sum(1 for c in cleaners if c['same_day'])}"
        ),
    ]

    # Find or create dashboard page
    search = notion_request("POST", "/search", {
        "query": DASHBOARD_TITLE,
        "filter": {"property": "object", "value": "page"},
    })
    dashboard_id = None
    if search:
        for result in search.get("results", []):
            title_arr = result.get("properties", {}).get("title", {}).get("title", [])
            if title_arr and title_arr[0]["plain_text"] == DASHBOARD_TITLE:
                parent = result.get("parent", {})
                if parent.get("page_id") == CLAUDE_WORKSHOP_ID:
                    dashboard_id = result["id"]
                    break

    if dashboard_id:
        # Clear existing content
        children = notion_request("GET", f"/blocks/{dashboard_id}/children")
        if children:
            for block in children.get("results", []):
                notion_request("DELETE", f"/blocks/{block['id']}")
                time.sleep(0.3)
        # Append new content
        # Notion limits append to 100 blocks at a time
        for i in range(0, len(blocks), 100):
            notion_request("PATCH", f"/blocks/{dashboard_id}/children", {
                "children": blocks[i:i+100]
            })
        print(f"Updated existing dashboard: {dashboard_id}")
    else:
        # Create new page
        page = notion_request("POST", "/pages", {
            "parent": {"page_id": CLAUDE_WORKSHOP_ID},
            "properties": {
                "title": {"title": [{"text": {"content": DASHBOARD_TITLE}}]}
            },
            "children": blocks[:100],
        })
        if page:
            dashboard_id = page["id"]
            # Append remaining blocks if > 100
            for i in range(100, len(blocks), 100):
                notion_request("PATCH", f"/blocks/{dashboard_id}/children", {
                    "children": blocks[i:i+100]
                })
            print(f"Created dashboard: {dashboard_id}")

    print("Done!")


if __name__ == "__main__":
    main()
