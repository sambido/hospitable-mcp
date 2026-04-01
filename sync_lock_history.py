#!/usr/bin/env python3
"""
Sync Lock Activity from Home Assistant + Hospitable to Notion.

Pulls lock events from HA, correlates with Hospitable reservations,
classifies events (cleaner / guest / admin), and writes to the
Lock Activity Notion database.

Runs daily at 8pm PT. Safe to re-run — deduplicates by reservation ID + type.
"""

import json, os, re, ssl, sys, time, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Los_Angeles")

# --- Config ---
LOCK_ACTIVITY_DB_ID = "33450c17-99cc-81e0-b025-f81a69944156"
STR_LISTINGS_DB_ID = "1eb50c17-99cc-8091-a8ea-e0ba6ec649ff"
CLEANING_TEAMS_DB_ID = "33450c17-99cc-810c-96ce-d0da64fad92e"
HOSPITABLE_BASE = "https://public.api.hospitable.com/v2"

# How many days back to look for checkouts (override with LOOKBACK_DAYS env var)
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

# Load tokens
HA_TOKEN = os.environ.get("HA_TOKEN", "").strip()
HA_URL = os.environ.get("HA_URL", "").strip().rstrip("/")
HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "").strip()
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()

if not HOSPITABLE_PAT or not NOTION_TOKEN or not HA_TOKEN or not HA_URL:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)
        HA_TOKEN = HA_TOKEN or os.environ.get("HA_TOKEN", "")
        HA_URL = (HA_URL or os.environ.get("HA_URL", "")).rstrip("/")
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

# --------------------------------------------------------------------------- #
# Property mappings
# --------------------------------------------------------------------------- #

# Hospitable UUID -> friendly name
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
    "d92cdc13-8bd9-4803-a277-55f8ba36bd29": "Palatine Lower",
    "5a3010a8-602d-4d79-9fa0-18f99d02fb88": "Dara",
}

# Hospitable UUID -> Notion STR Listings page ID
PROPERTY_NOTION_IDS = {
    "92a1c198-4d3e-4d1b-a5f8-e90f98f1c49c": "32050c17-99cc-81d1-8434-e4366b86acc0",
    "13a74151-c6bc-434b-8de1-549f048d77c7": "32050c17-99cc-819a-8271-e687bb4f6e62",
    "f1970a87-2c41-4cd8-b222-329980b45a78": "32050c17-99cc-81b7-82f7-ed78866ed7f8",
    "c50f431b-1d44-40fd-8788-92708710a1cc": "32050c17-99cc-81b0-912f-c90506efd195",
    "f3fd4981-3f21-4c5a-8888-ba259834ddb5": "1eb50c17-99cc-802c-893d-c1f7599e67ce",
    "bd0528ad-c1cb-4035-821a-fb1199dfacaa": "1eb50c17-99cc-8056-8a51-cc3c3e3acc73",
    "d708140c-4ba0-4673-ba44-0b11d4f97181": "32050c17-99cc-8106-9965-d054efc61dbc",
    "c80e149c-0ae3-4cf1-965b-5fd12e97f7f6": "32050c17-99cc-8176-bc72-c9721385f44c",
    "bef6a386-1446-4c09-a7db-757824cd6d35": "32050c17-99cc-81b9-bef1-c14437a90c7f",
    "ab7b6a1b-b731-4046-8406-654a3b62b2cb": "1eb50c17-99cc-8000-8105-d27281de47a4",
    "10bd7b2b-e250-416f-b45f-a1a4d0e92e3c": "32050c17-99cc-8191-882d-e32da5c56759",
    "eefb5918-5149-4b4e-bdd0-277754409cb0": "32050c17-99cc-8188-9bfd-f23a4cc8c028",
    "56ea4fe3-3445-4a6b-962f-a02cbbd2869b": "32050c17-99cc-81cd-af07-fc5780b95a81",
    "123ee545-ddf9-4e25-b6d0-e597afc5612b": "32050c17-99cc-8177-8f32-eed75d037b43",
    "5cf63104-6ae7-40b2-aa7d-c10d18822ccd": "1eb50c17-99cc-8020-a42f-ea82500a4099",
    "9bfda321-b0f0-4c4e-8f03-eeb86ef3c87f": "1d550c17-99cc-80e2-9c4c-d32b89ccd7f2",
    "4dbf5125-6efe-4097-90f6-3fab87a911d2": "32050c17-99cc-8138-8e44-d992e3009dd7",
    "14912b54-f5e0-47ac-a8c2-1e1d9e17bbd6": "32050c17-99cc-816a-96da-d923599db53e",
    "8423a091-1513-4d98-9e68-c6c3888b1f9e": "32050c17-99cc-81cc-9972-d8677481a3ee",
    "a8cd20bc-16f9-44d0-8c3f-12bea51720cb": "31f50c17-99cc-8110-9d0f-fe7c5b2ae44b",
    "c84923ff-a37b-4463-93d6-d192de05be78": "32050c17-99cc-81fe-b497-eb7417ca5849",
    "df375ad6-b2e8-43de-a7f2-45d658864736": "32050c17-99cc-8196-b3d1-c41c2355904d",
    "d92cdc13-8bd9-4803-a277-55f8ba36bd29": "32050c17-99cc-811a-8992-ef25a487d6d1",
    "5a3010a8-602d-4d79-9fa0-18f99d02fb88": "32050c17-99cc-8152-991c-c0ec9b36be7c",
}

# Hospitable UUID -> list of HA lock entity IDs
PROPERTY_LOCKS = {
    "92a1c198-4d3e-4d1b-a5f8-e90f98f1c49c": [  # 65th
        "lock.1025nw65thst_landing", "lock.1025nw65thst_basement", "lock.1025nw65thst",
    ],
    "f3fd4981-3f21-4c5a-8888-ba259834ddb5": [  # 8th
        "lock.7301_front", "lock.7301_kitchen", "lock.8th_basement",
    ],
    "4dbf5125-6efe-4097-90f6-3fab87a911d2": [  # Bridget
        "lock.1807nw73rdst_front_door", "lock.1807nw73rdst_side_door",
        "lock.1807nw73rdst_basement_door",
    ],
    "123ee545-ddf9-4e25-b6d0-e597afc5612b": [  # Jeremy
        "lock.551532ndavenw_front_door", "lock.551532ndavenw_kitchen_door",
    ],
    "c50f431b-1d44-40fd-8788-92708710a1cc": [  # Andy
        "lock.1022ne72ndst", "lock.storage_closet_1022_lock",
    ],
    "d708140c-4ba0-4673-ba44-0b11d4f97181": [  # Lia
        "lock.lia_11211_fremont",
    ],
    "8423a091-1513-4d98-9e68-c6c3888b1f9e": [  # Michael
        "lock.36081stavenw",
    ],
    "bef6a386-1446-4c09-a7db-757824cd6d35": [  # Eve
        "lock.eve_front_door", "lock.eve_deck_door", "lock.eve_downstairs_door",
    ],
    "bd0528ad-c1cb-4035-821a-fb1199dfacaa": [  # Chris (Upper Fremont)
        "lock.4260fremontaven", "lock.4260fremontaven_back",
        "lock.fremont_bldg_front", "lock.fremont_bldg_back",
    ],
    "13a74151-c6bc-434b-8de1-549f048d77c7": [  # Gunny
        "lock.2317elynnst",
    ],
    "c80e149c-0ae3-4cf1-965b-5fd12e97f7f6": [  # Sophia
        "lock.284623rdavew_front_door", "lock.284623rdavew_back_door",
        "lock.sophia_basement_laundry",
    ],
    "ab7b6a1b-b731-4046-8406-654a3b62b2cb": [  # Assim
        "lock.assim",
    ],
    "10bd7b2b-e250-416f-b45f-a1a4d0e92e3c": [  # Susan
        "lock.susan_mercer_island",
    ],
    "56ea4fe3-3445-4a6b-962f-a02cbbd2869b": [  # Matthew
        "lock.92810thaveeb",
    ],
    "9bfda321-b0f0-4c4e-8f03-eeb86ef3c87f": [  # Sundee
        "lock.6240sycamoreavenw", "lock.6240sycamoreavenw_back_door",
    ],
    "5a3010a8-602d-4d79-9fa0-18f99d02fb88": [  # Dara
        "lock.jeff_and_dara",
    ],
    "5cf63104-6ae7-40b2-aa7d-c10d18822ccd": [  # Don and Kathy
        "lock.don_and_kathy",
    ],
    "14912b54-f5e0-47ac-a8c2-1e1d9e17bbd6": [  # Adam
        "lock.4610sbrightonst",
    ],
    "c84923ff-a37b-4463-93d6-d192de05be78": [  # Danial
        "lock.uptown_mercer_place_front_door",
    ],
    "eefb5918-5149-4b4e-bdd0-277754409cb0": [  # Chad
        "lock.shared_roof_302",
    ],
    "d92cdc13-8bd9-4803-a277-55f8ba36bd29": [  # Palatine (all locks shared across parent-child)
        "lock.palatine_front_door", "lock.palatine_lower_unit",
        "lock.palatine_upper_kitchen", "lock.palatine_side_bldg",
    ],
    "f1970a87-2c41-4cd8-b222-329980b45a78": [  # Mary Anne (hiatus until June)
        "lock.little_house_front_door", "lock.little_house_courtyard",
        "lock.little_house_garage",
    ],
    # Nordic Loft (lock.dadu) — not live yet, omitted
    # Miller Bay — no lock entities in HA
}

# Sam's own property (for personal use, not in sync loop but mapped)
SAM_PROPERTY_LOCKS = {
    "725901a7-f4a4-4892-866b-df14dc8f4ff7": [  # 32nd (Sam)
        "lock.7115_front_door", "lock.7115_basement", "lock.7115_mudroom",
    ],
}

# Locks to EXCLUDE from cleaning session detection (building/storage locks).
# These are still pulled for history but ignored when calculating session duration.
EXCLUDED_SESSION_LOCKS = {
    "lock.fremont_bldg_front",      # Chris — building common door
    "lock.fremont_bldg_back",       # Chris — building common door
    "lock.storage_closet_1022_lock", # Andy — storage closet
}

# Known cleaner code patterns
CLEANER_PATTERNS = [
    "cleaner:",
    "jay's cleaners",
    "jiselle",
    "ana guayllas",
    "ana clean",
    "gilda",
]

# Property-specific cleaner overrides (owners who clean their own property)
# Maps Hospitable UUID -> list of changed_by substrings that mean "cleaner"
OWNER_CLEANER_OVERRIDES = {
    "5cf63104-6ae7-40b2-aa7d-c10d18822ccd": ["don", "kathy"],  # Don and Kathy
}

# Known admin/owner patterns
ADMIN_PATTERNS = [
    "admin",
    "co-host",
    "mobile device",
    "weatherwood",
    "owner:",
]

# Hospitable guest code pattern: "Name Code123" (first name + space + alphanumeric 3-5 chars)
GUEST_CODE_RE = re.compile(r"^keypad - .+ [A-Za-z0-9]{3,6}$")
HOSP_CODE_RE = re.compile(r"^keypad - HOSP[A-Za-z0-9]+$")

# Manual/auto patterns (not attributable to a person)
MANUAL_PATTERNS = ["thumbturn", "1-touch locking", "unknown"]

# Cleaning Teams Notion page IDs (for relation field)
CLEANING_TEAM_IDS = {
    "ana": "33450c17-99cc-8140-8fef-f8ef05477fba",
    "jiselle": "33450c17-99cc-817f-a902-dfd903aacdf5",
    "gilda": "33450c17-99cc-8120-8f5e-d4c28bd6f5e7",
    "don and kathy": "33450c17-99cc-8123-94dc-c76384dc4328",
}

# Map raw changed_by person names to canonical team keys
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


def get_cleaning_team_id(person_name):
    """Look up the Cleaning Teams Notion page ID for a cleaner person name."""
    if not person_name:
        return None
    team_key = CLEANER_TEAM_MAP.get(person_name.lower().strip())
    if team_key:
        return CLEANING_TEAM_IDS.get(team_key)
    return None


# --------------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------------- #
def classify_changed_by(changed_by, prop_uuid):
    """Classify a changed_by value as cleaner/guest/admin/manual.
    Returns (type, person_name)."""
    if not changed_by or changed_by in ("unknown", "N/A", ""):
        return "manual", None

    cb_lower = changed_by.lower().strip()

    # Manual/auto
    for pat in MANUAL_PATTERNS:
        if cb_lower == pat:
            return "manual", None

    # Strip "keypad - " or "mobile device - " prefix for person name
    person = changed_by
    for prefix in ("keypad - ", "mobile device - "):
        if person.startswith(prefix):
            person = person[len(prefix):]
            break

    # Property-specific cleaner overrides
    overrides = OWNER_CLEANER_OVERRIDES.get(prop_uuid, [])
    for override in overrides:
        if override in cb_lower:
            return "cleaner", person

    # Known cleaner patterns
    for pat in CLEANER_PATTERNS:
        if pat in cb_lower:
            return "cleaner", person

    # Admin/owner patterns
    for pat in ADMIN_PATTERNS:
        if pat in cb_lower:
            return "admin", person

    # Hospitable auto-generated guest codes (HOSP prefix)
    if HOSP_CODE_RE.match(changed_by):
        return "guest", person

    # Guest codes: "FirstName AlphanumericSuffix" pattern
    if GUEST_CODE_RE.match(changed_by):
        return "guest", person

    # Check if changed_by matches the property name (owner/property code)
    prop_name = PROPERTIES.get(prop_uuid, "").lower()
    if prop_name and prop_name in cb_lower:
        return "admin", person

    # Unclassified — treat as guest (most likely) but flag
    return "guest", person


# --------------------------------------------------------------------------- #
# API helpers
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
                print(f"  Notion {e.code}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Notion error {e.code}: {e.read().decode()[:200]}")
                return None
    return None


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


def ha_get_history(entity_ids, start_dt, end_dt):
    """Pull lock history from Home Assistant for given entity IDs and time range."""
    entity_filter = ",".join(entity_ids)
    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()
    url = f"{HA_URL}/api/history/period/{start_iso}?filter_entity_id={entity_filter}&end_time={end_iso}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "WeatherwoodSync/1.0",
    })
    try:
        resp = urllib.request.urlopen(req, context=CTX)
        return json.loads(resp.read())
    except Exception as e:
        print(f"  HA API error: {e}")
        return []


# --------------------------------------------------------------------------- #
# Hospitable helpers
# --------------------------------------------------------------------------- #
def fetch_reservations(property_uuid, start_date, end_date):
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


# --------------------------------------------------------------------------- #
# Lock event processing
# --------------------------------------------------------------------------- #
# entity_id -> friendly name, populated from HA history responses at runtime
LOCK_FRIENDLY_NAMES = {}


def lock_display_name(entity_id):
    """Return the friendly name for a lock entity, falling back to entity_id."""
    return LOCK_FRIENDLY_NAMES.get(entity_id, entity_id)


def parse_ha_events(history_data):
    """Parse HA history API response into flat list of events sorted by time."""
    events = []
    for entity_events in history_data:
        for entry in entity_events:
            # Capture friendly_name from HA attributes (populates once per entity)
            eid = entry.get("entity_id", entity_events[0].get("entity_id", ""))
            if eid and eid not in LOCK_FRIENDLY_NAMES:
                fname = entry.get("attributes", {}).get("friendly_name", "")
                if fname:
                    LOCK_FRIENDLY_NAMES[eid] = fname

            state = entry.get("state", "")
            if state not in ("locked", "unlocked"):
                continue
            changed_by = entry.get("attributes", {}).get("changed_by", "")
            ts_str = entry.get("last_changed", "")
            if not ts_str:
                continue
            try:
                ts = datetime.fromisoformat(ts_str).astimezone(PACIFIC)
            except ValueError:
                continue
            events.append({
                "timestamp": ts,
                "state": state,
                "changed_by": changed_by or "",
                "entity_id": eid,
            })
    events.sort(key=lambda e: e["timestamp"])
    return events


def find_cleaning_session(events, checkout_dt, checkin_dt, prop_uuid):
    """Find the cleaning session between checkout and next check-in.

    Logic: once a cleaner unlocks, all subsequent events (including thumbturn,
    1-touch locking, etc.) are part of the cleaning session until:
    - A guest code is used (next guest arrived)
    - A gap of >3 hours occurs (cleaner left, separate activity later)
    - We hit the next check-in time
    """
    # First, find the first cleaner unlock after checkout
    session_start = None
    cleaner_person = None
    in_session = False
    session_events = []
    entities_used = set()

    for evt in events:
        if evt["timestamp"] < checkout_dt:
            continue
        if checkin_dt and evt["timestamp"] > checkin_dt:
            break
        # Skip building/storage locks for session detection
        if evt["entity_id"] in EXCLUDED_SESSION_LOCKS:
            continue

        etype, person = classify_changed_by(evt["changed_by"], prop_uuid)

        if not in_session:
            # Looking for the cleaner to arrive
            if etype == "cleaner":
                in_session = True
                session_start = evt
                cleaner_person = person
                session_events.append(evt)
                entities_used.add(evt["entity_id"])
        else:
            # In a cleaning session — include all events until session ends
            if etype == "guest":
                # Next guest arrived, session is over
                break
            # Check for >90 min gap (cleaner left, separate visit later)
            if session_events and (evt["timestamp"] - session_events[-1]["timestamp"]).total_seconds() > 5400:
                break
            session_events.append(evt)
            entities_used.add(evt["entity_id"])

    if not session_events:
        return None

    first = session_events[0]
    last = session_events[-1]
    duration = int((last["timestamp"] - first["timestamp"]).total_seconds() / 60)

    minutes_after_checkout = int((first["timestamp"] - checkout_dt).total_seconds() / 60)
    minutes_before_checkin = None
    if checkin_dt:
        minutes_before_checkin = int((checkin_dt - last["timestamp"]).total_seconds() / 60)

    return {
        "person": cleaner_person or "Unknown Cleaner",
        "first_event": first["timestamp"],
        "last_event": last["timestamp"],
        "duration": max(duration, 0),
        "event_count": len(session_events),
        "minutes_after_checkout": max(minutes_after_checkout, 0),
        "minutes_before_checkin": minutes_before_checkin,
        "entities_used": ", ".join(sorted(entities_used)),
        "lock_names": ", ".join(sorted(lock_display_name(e) for e in entities_used)),
    }


def find_guest_activity(events, checkin_dt, checkout_dt, prop_uuid, guest_name):
    """Find guest activity during their stay."""
    guest_events = []
    entities_used = set()

    for evt in events:
        if evt["timestamp"] < checkin_dt:
            continue
        # Allow up to 2 hours after checkout to catch late checkouts
        if evt["timestamp"] > checkout_dt + timedelta(hours=2):
            break
        # Skip building/storage locks for guest activity
        if evt["entity_id"] in EXCLUDED_SESSION_LOCKS:
            continue
        etype, _ = classify_changed_by(evt["changed_by"], prop_uuid)
        if etype == "guest":
            guest_events.append(evt)
            entities_used.add(evt["entity_id"])

    if not guest_events:
        return {
            "person": guest_name,
            "first_event": None,
            "last_event": None,
            "duration_hours": None,
            "event_count": 0,
            "no_show": True,
            "late_checkout": False,
            "entities_used": "",
        "lock_names": "",
        }

    first = guest_events[0]
    last = guest_events[-1]
    duration_hours = round((last["timestamp"] - first["timestamp"]).total_seconds() / 3600, 1)

    # Late checkout: last guest event > 30min after scheduled checkout
    late_checkout = last["timestamp"] > checkout_dt + timedelta(minutes=30)

    return {
        "person": guest_name,
        "first_event": first["timestamp"],
        "last_event": last["timestamp"],
        "duration_hours": max(duration_hours, 0),
        "event_count": len(guest_events),
        "no_show": False,
        "late_checkout": late_checkout,
        "entities_used": ", ".join(sorted(entities_used)),
        "lock_names": ", ".join(sorted(lock_display_name(e) for e in entities_used)),
    }


# --------------------------------------------------------------------------- #
# Notion operations
# --------------------------------------------------------------------------- #
def get_existing_entries():
    """Get existing lock activity entries, keyed by 'reservation_id|type'."""
    existing = {}
    has_more = True
    start_cursor = None
    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        result = notion_request("POST", f"/databases/{LOCK_ACTIVITY_DB_ID}/query", payload)
        if not result:
            break
        for page in result.get("results", []):
            res_rt = page["properties"].get("Reservation ID", {}).get("rich_text", [])
            type_sel = page["properties"].get("Type", {}).get("select")
            if res_rt and type_sel:
                key = f"{res_rt[0]['plain_text']}|{type_sel['name']}"
                existing[key] = page["id"]
        has_more = result.get("has_more", False)
        start_cursor = result.get("next_cursor")
    return existing


def build_notion_props(entry_type, prop_uuid, prop_name, data, res_code,
                       checkout_dt, checkin_dt, same_day):
    """Build Notion properties dict for a lock activity entry."""
    date_str = checkout_dt.strftime("%Y-%m-%d")
    person = data.get("person", "Unknown")
    title = f"{person} — {prop_name} — {date_str}"

    props = {
        "Name": {"title": [{"text": {"content": title[:100]}}]},
        "Type": {"select": {"name": entry_type}},
        "Person": {"rich_text": [{"text": {"content": person[:100]}}]},
        "Reservation ID": {"rich_text": [{"text": {"content": str(res_code)}}]},
        "Same Day Turnover": {"checkbox": same_day},
        "Scheduled Checkout": {"date": {"start": checkout_dt.isoformat()}},
    }

    notion_page_id = PROPERTY_NOTION_IDS.get(prop_uuid)
    if notion_page_id:
        props["Property"] = {"relation": [{"id": notion_page_id}]}

    # Link cleaner rows to Cleaning Teams database
    if entry_type == "Cleaner":
        team_id = get_cleaning_team_id(data.get("person", ""))
        if team_id:
            props["Cleaning Team"] = {"relation": [{"id": team_id}]}

    if checkin_dt:
        props["Scheduled Check-in"] = {"date": {"start": checkin_dt.isoformat()}}

    if data.get("first_event"):
        props["First Event"] = {"date": {"start": data["first_event"].isoformat()}}
    if data.get("last_event"):
        props["Last Event"] = {"date": {"start": data["last_event"].isoformat()}}

    # Compute all three duration formats from total minutes
    total_min = data.get("duration")  # cleaners have this
    if total_min is None and data.get("duration_hours") is not None:
        total_min = round(data["duration_hours"] * 60)

    if total_min is not None:
        props["Duration (min)"] = {"number": total_min}
        props["Duration (hrs)"] = {"number": round(total_min / 60, 1)}
        days = total_min // 1440
        hrs = (total_min % 1440) // 60
        mins = total_min % 60
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hrs > 0:
            parts.append(f"{hrs}hr")
        if mins > 0:
            parts.append(f"{mins}min")
        props["Duration (formatted)"] = {"rich_text": [{"text": {"content": ", ".join(parts) or "0min"}}]}

    if data.get("event_count") is not None:
        props["Event Count"] = {"number": data["event_count"]}
    if data.get("entities_used"):
        props["Lock Entities Used"] = {"rich_text": [{"text": {"content": data["entities_used"][:200]}}]}
    if data.get("lock_names"):
        props["Lock Names"] = {"rich_text": [{"text": {"content": data["lock_names"][:200]}}]}

    if entry_type == "Cleaner":
        if data.get("minutes_after_checkout") is not None:
            props["Minutes After Checkout"] = {"number": data["minutes_after_checkout"]}
        if data.get("minutes_before_checkin") is not None:
            props["Minutes Before Check-in"] = {"number": data["minutes_before_checkin"]}
        # Auto-generate notes for cleaners
        notes = []
        if same_day:
            notes.append("same-day turnover")
        dur = data.get("duration", 0)
        if dur > 240:
            notes.append(f"long clean ({dur}min)")
        if data.get("minutes_before_checkin") is not None and data["minutes_before_checkin"] < 60:
            notes.append(f"tight buffer ({data['minutes_before_checkin']}min before check-in)")
        props["Notes"] = {"rich_text": [{"text": {"content": "; ".join(notes)}}]}

    elif entry_type == "Guest":
        props["Late Checkout"] = {"checkbox": data.get("late_checkout", False)}
        props["No-Show"] = {"checkbox": data.get("no_show", False)}
        guest_notes = []
        if data.get("late_checkout") and data.get("last_event"):
            delay = int((data["last_event"] - checkout_dt).total_seconds() / 60)
            props["Minutes After Checkout"] = {"number": max(delay, 0)}
            guest_notes.append(f"late checkout ({delay}min past scheduled)")
        if data.get("no_show"):
            guest_notes.append("no lock events during stay")
        if guest_notes:
            props["Notes"] = {"rich_text": [{"text": {"content": "; ".join(guest_notes)}}]}

    return props


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    if not HA_TOKEN:
        print("ERROR: HA_TOKEN not set")
        sys.exit(1)
    if not HA_URL:
        print("ERROR: HA_URL not set")
        sys.exit(1)
    if not HOSPITABLE_PAT:
        print("ERROR: HOSPITABLE_PAT not set")
        sys.exit(1)
    if not NOTION_TOKEN:
        print("ERROR: NOTION_TOKEN not set")
        sys.exit(1)

    now = datetime.now(PACIFIC)
    today = now.strftime("%Y-%m-%d")
    lookback_start = (now - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    print(f"Lock Activity Sync — {lookback_start} to {today}")
    print(f"Properties with locks: {len(PROPERTY_LOCKS)}")

    # Get existing Notion entries for dedup
    existing = get_existing_entries()
    print(f"Existing entries in Notion: {len(existing)}")

    created = 0
    updated = 0
    skipped = 0

    for prop_uuid, lock_entities in PROPERTY_LOCKS.items():
        prop_name = PROPERTIES.get(prop_uuid, prop_uuid)
        print(f"\n--- {prop_name} ---")

        # Fetch reservations covering our lookback window
        fetch_start = (now - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")
        fetch_end = (now + timedelta(days=7)).strftime("%Y-%m-%d")

        try:
            reservations = fetch_reservations(prop_uuid, fetch_start, fetch_end)
        except Exception as e:
            print(f"  Error fetching reservations: {e}")
            continue

        if not reservations:
            print("  No reservations in window")
            continue

        reservations.sort(key=lambda r: r.get("arrival_date", ""))

        # Find checkouts in our lookback window
        for i, res in enumerate(reservations):
            departure = res.get("departure_date", "")[:10]
            arrival = res.get("arrival_date", "")[:10]
            res_code = res.get("code", res.get("id", ""))

            if not (lookback_start <= departure <= today):
                continue

            guest = res.get("guest", {}) or {}
            guest_name = f"{guest.get('first_name', '')} {guest.get('last_name', '')}".strip() or "Unknown"

            # Default checkout at 11am PT, check-in at 4pm PT
            checkout_dt = datetime.strptime(departure, "%Y-%m-%d").replace(
                hour=11, tzinfo=PACIFIC)
            checkin_dt = datetime.strptime(arrival, "%Y-%m-%d").replace(
                hour=16, tzinfo=PACIFIC)

            # Find next reservation for same-day turnover detection
            next_checkin_dt = None
            same_day = False
            if i + 1 < len(reservations):
                next_res = reservations[i + 1]
                next_arrival = next_res.get("arrival_date", "")[:10]
                if next_arrival:
                    next_checkin_dt = datetime.strptime(next_arrival, "%Y-%m-%d").replace(
                        hour=16, tzinfo=PACIFIC)
                    same_day = (next_arrival == departure)

            print(f"  Checkout: {departure} — {guest_name}" +
                  (" [same-day]" if same_day else ""))

            # Pull lock history: from guest check-in to next check-in (or +2 days)
            history_start = checkin_dt - timedelta(hours=1)
            history_end = next_checkin_dt + timedelta(hours=2) if next_checkin_dt else checkout_dt + timedelta(days=2)

            history = ha_get_history(lock_entities, history_start, history_end)
            events = parse_ha_events(history) if history else []
            print(f"    Lock events: {len(events)}")

            # --- Cleaner row ---
            cleaner_key = f"{res_code}|Cleaner"
            cleaning = find_cleaning_session(events, checkout_dt,
                                             next_checkin_dt or checkout_dt + timedelta(days=1),
                                             prop_uuid)
            if cleaning:
                props = build_notion_props("Cleaner", prop_uuid, prop_name, cleaning,
                                           res_code, checkout_dt, next_checkin_dt, same_day)
                page_id = existing.get(cleaner_key)
                if page_id:
                    notion_request("PATCH", f"/pages/{page_id}", {"properties": props})
                    updated += 1
                    print(f"    Cleaner: updated ({cleaning['person']}, {cleaning['duration']}min)")
                else:
                    payload = {"parent": {"database_id": LOCK_ACTIVITY_DB_ID}, "properties": props}
                    notion_request("POST", "/pages", payload)
                    created += 1
                    print(f"    Cleaner: created ({cleaning['person']}, {cleaning['duration']}min)")
                time.sleep(0.4)
            else:
                print("    Cleaner: no cleaning session detected")

            # --- Guest row ---
            guest_key = f"{res_code}|Guest"
            guest_data = find_guest_activity(events, checkin_dt, checkout_dt,
                                             prop_uuid, guest_name)
            props = build_notion_props("Guest", prop_uuid, prop_name, guest_data,
                                       res_code, checkout_dt, next_checkin_dt, same_day)
            # For guest rows, Scheduled Check-in = their check-in, Scheduled Checkout = their checkout
            props["Scheduled Check-in"] = {"date": {"start": checkin_dt.isoformat()}}
            props["Scheduled Checkout"] = {"date": {"start": checkout_dt.isoformat()}}

            page_id = existing.get(guest_key)
            if page_id:
                notion_request("PATCH", f"/pages/{page_id}", {"properties": props})
                updated += 1
                status = "no-show" if guest_data.get("no_show") else f"{guest_data['event_count']} events"
                print(f"    Guest: updated ({guest_name}, {status})")
            else:
                payload = {"parent": {"database_id": LOCK_ACTIVITY_DB_ID}, "properties": props}
                notion_request("POST", "/pages", payload)
                created += 1
                status = "no-show" if guest_data.get("no_show") else f"{guest_data['event_count']} events"
                print(f"    Guest: created ({guest_name}, {status})")
            time.sleep(0.4)

    print(f"\n--- Done ---")
    print(f"Created: {created}, Updated: {updated}")


if __name__ == "__main__":
    main()
