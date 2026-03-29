#!/usr/bin/env python3
"""Create Turnover Schedule database in Notion under Claude Workshop."""

import json
import os
import urllib.request
import urllib.error

# Claude Workshop page (staging area for all new databases)
PARENT_PAGE_ID = "32d50c17-99cc-80a9-97cb-d07b2be142c8"
STR_LISTINGS_DB_ID = "1eb50c17-99cc-8091-a8ea-e0ba6ec649ff"

# Load Notion token -- env var first, then ~/.claude.json
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
if not NOTION_TOKEN:
    try:
        with open(os.path.expanduser("~/.claude.json")) as f:
            _cfg = json.load(f)
        _headers_str = _cfg["mcpServers"]["notionApi"]["env"]["OPENAPI_MCP_HEADERS"]
        NOTION_TOKEN = json.loads(_headers_str)["Authorization"].replace("Bearer ", "")
    except (FileNotFoundError, KeyError):
        pass

if not NOTION_TOKEN:
    print("ERROR: No Notion token found")
    exit(1)


def notion_request(method, path, payload=None):
    url = f"https://api.notion.com/v1{path}"
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code}: {e.read().decode()}")
        raise


payload = {
    "parent": {"type": "page_id", "page_id": PARENT_PAGE_ID},
    "title": [{"type": "text", "text": {"content": "Turnover Schedule"}}],
    "properties": {
        "Name": {"title": {}},
        "Property": {
            "relation": {
                "database_id": STR_LISTINGS_DB_ID,
                "type": "single_property",
                "single_property": {},
            }
        },
        "Guest Name": {"rich_text": {}},
        "Checkout Date": {"date": {}},
        "Checkout Time": {
            "select": {
                "options": [
                    {"name": "Default", "color": "gray"},
                ]
            }
        },
        "Next Check-in Date": {"date": {}},
        "Check-in Time": {
            "select": {
                "options": [
                    {"name": "Default", "color": "gray"},
                ]
            }
        },
        "Next Guest Name": {"rich_text": {}},
        "Time Source": {
            "select": {
                "options": [
                    {"name": "Guest message", "color": "green"},
                    {"name": "Default", "color": "gray"},
                ]
            }
        },
        "Cleaning Team": {
            "select": {
                "options": [
                    {"name": "Ana", "color": "blue"},
                    {"name": "Jiselle", "color": "purple"},
                    {"name": "Unassigned", "color": "gray"},
                ]
            }
        },
        "Reservation ID": {"rich_text": {}},
        "Notes": {"rich_text": {}},
    },
}

print("Creating Turnover Schedule database...")
result = notion_request("POST", "/databases", payload)
db_id = result["id"]
db_url = result["url"]
print(f"Created: {db_url}")
print(f"  DB ID: {db_id}")
