#!/usr/bin/env python3
"""
Sync guest messages from Hospitable → classify with Claude API → push to Notion.

Classifies messages into:
  - FAQ → Guest FAQs database
  - Maintenance → To-Do / Maintenance database
  - Guest Request → Guest Requests database
  - Skip → not actionable

Runs hourly via GitHub Actions. State persisted in Notion (Sync State page).
"""

import json, os, ssl, time, urllib.request, urllib.error
from datetime import datetime, timedelta

# --------------------------------------------------------------------------- #
# Config — env vars first (GitHub Actions), then local files
# --------------------------------------------------------------------------- #
HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

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
            _h = _cfg["mcpServers"]["notionApi"]["env"]["OPENAPI_MCP_HEADERS"]
            NOTION_TOKEN = json.loads(_h)["Authorization"].replace("Bearer ", "")
        except (FileNotFoundError, KeyError):
            pass

HOSPITABLE_BASE = "https://public.api.hospitable.com/v2"
CTX = ssl.create_default_context()

# Notion database IDs
FAQ_DB = "32550c17-99cc-8153-9499-c360b75733b1"
MAINTENANCE_DB = "32550c17-99cc-8102-bcce-c3c10770785d"
GUEST_REQUESTS_DB = "32450c17-99cc-8105-a317-f01a118f7a73"
STR_LISTINGS_DB = "1eb50c17-99cc-8091-a8ea-e0ba6ec649ff"

# Sync state — stored as a Notion page so GitHub Actions can persist across runs
SYNC_STATE_PAGE = os.environ.get("SYNC_STATE_PAGE", "32c50c17-99cc-8186-b03b-d9287547feff")

# Claude model for classification
CLAUDE_MODEL = "claude-sonnet-4-20250514"


# --------------------------------------------------------------------------- #
# API helpers
# --------------------------------------------------------------------------- #
def notion_request(method, endpoint, data=None):
    url = f"https://api.notion.com/v1{endpoint}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method, headers={
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    })
    for attempt in range(3):
        try:
            resp = urllib.request.urlopen(req, context=CTX)
            return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(5)
            else:
                print(f"  Notion error {e.code}: {e.read().decode()[:200]}")
                return None
    return None


def hospitable_get(endpoint, params=None):
    url = f"{HOSPITABLE_BASE}{endpoint}"
    if params:
        parts = []
        for k, v in params.items():
            parts.append(f"{k}={v}")
        url += "?" + "&".join(parts)
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {HOSPITABLE_PAT}",
        "Accept": "application/json",
    })
    resp = urllib.request.urlopen(req, context=CTX)
    return json.loads(resp.read())


def claude_classify(messages_batch, existing_faqs):
    """Send a batch of guest messages to Claude for classification."""
    faq_names = [f["name"] for f in existing_faqs]

    system_prompt = """You are a classifier for a short-term rental property manager.
You will receive guest messages from Airbnb/VRBO reservations. For each message, determine:

1. TYPE: One of:
   - "faq" — a question or concern other guests might also have (amenities, parking, check-in, wifi, appliances, local tips, property features)
   - "maintenance" — reports a broken/damaged/malfunctioning item or a property issue that needs fixing
   - "guest_request" — a specific request for this stay (early check-in, extra towels, package delivery, etc.)
   - "skip" — not actionable: pure logistics ("arriving at 6pm"), confirmations ("ok thanks"), emoji-only, compliments, checkout messages, payment/billing questions

2. QUESTION: The core question or issue, distilled into a clean canonical form.
   For FAQ: "Is there a hair dryer?" not "hey do you guys happen to have a hair dryer?"
   For Maintenance: "Dishwasher not draining" not "so the dishwasher seems to have some issue"
   For Guest Request: "Early check-in request" not "would it be possible to maybe come a bit early"

3. CATEGORY: Best fit from these lists:
   FAQ categories: Check-In, Check-Out, Parking & Transportation, Wifi & Tech, Kitchen, Cleaning & Laundry, Bedding & Linens, Baby & Family, Pets, Outdoor Spaces, Local Area & Dining, Property Layout, Safety & Emergencies, House Rules & Policies, Activities & Attractions
   Maintenance categories: Plumbing, Electrical, Appliance, HVAC, Structural, Cleaning, Pest, Exterior, Safety, Other
   Guest Request categories: Early Check-in, Late Check-out, Extra Supplies, Special Occasion, Package Delivery, Transportation, Other

4. EXISTING_MATCH: If type is "faq", check if it semantically matches any of these existing FAQ names (same topic, different wording counts as a match). Return the exact matched name or null if no match.

Respond with a JSON array, one object per message. Example:
[{"index": 0, "type": "faq", "question": "Is there parking available?", "category": "Parking & Transportation", "existing_match": "Where can I park?"}]"""

    user_content = "EXISTING FAQ NAMES:\n" + json.dumps(faq_names) + "\n\nMESSAGES TO CLASSIFY:\n"
    for i, msg in enumerate(messages_batch):
        user_content += f"\n[{i}] Property: {msg['property_name']}\nMessage: {msg['body']}\n"

    body = json.dumps({
        "model": CLAUDE_MODEL,
        "max_tokens": 2048,
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
    resp = urllib.request.urlopen(req, context=CTX)
    result = json.loads(resp.read())

    # Extract text content and parse JSON
    text = result["content"][0]["text"]
    # Find JSON array in response
    start = text.find("[")
    end = text.rfind("]") + 1
    if start >= 0 and end > start:
        return json.loads(text[start:end])
    return []


# --------------------------------------------------------------------------- #
# State management (Notion-based)
# --------------------------------------------------------------------------- #
def get_last_run():
    """Get the last run timestamp from Notion page block content."""
    if SYNC_STATE_PAGE:
        try:
            result = notion_request("GET", f"/blocks/{SYNC_STATE_PAGE}/children")
            if result:
                for block in result.get("results", []):
                    if block["type"] == "paragraph":
                        texts = block["paragraph"].get("rich_text", [])
                        if texts:
                            state = json.loads(texts[0]["plain_text"])
                            return state.get("last_run", "")
        except Exception:
            pass
    # Default: 1 hour ago
    return (datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def update_last_run(timestamp, messages_processed):
    """Update the sync state in Notion page block."""
    if SYNC_STATE_PAGE:
        try:
            result = notion_request("GET", f"/blocks/{SYNC_STATE_PAGE}/children")
            if result and result.get("results"):
                block_id = result["results"][0]["id"]
                state = json.dumps({"last_run": timestamp, "messages_processed": messages_processed})
                notion_request("PATCH", f"/blocks/{block_id}", {
                    "paragraph": {"rich_text": [{"text": {"content": state}}]}
                })
        except Exception as e:
            print(f"  Error updating state: {e}")


# --------------------------------------------------------------------------- #
# Property lookup
# --------------------------------------------------------------------------- #
def get_property_notion_ids():
    """Map Hospitable UUIDs to Notion page IDs from STR Listings."""
    mapping = {}
    result = notion_request("POST", f"/databases/{STR_LISTINGS_DB}/query", {"page_size": 100})
    if not result:
        return mapping
    for page in result.get("results", []):
        uuid_field = page["properties"].get("UUID_Hospitable", {}).get("rich_text", [])
        if uuid_field:
            mapping[uuid_field[0]["plain_text"]] = page["id"]
    return mapping


# --------------------------------------------------------------------------- #
# Existing items
# --------------------------------------------------------------------------- #
def get_existing_faqs():
    """Load all existing FAQ names for semantic matching."""
    faqs = []
    start_cursor = None
    while True:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        result = notion_request("POST", f"/databases/{FAQ_DB}/query", payload)
        if not result:
            break
        for page in result.get("results", []):
            title = page["properties"].get("Name", {}).get("title", [])
            name = title[0]["plain_text"] if title else ""
            freq = page["properties"].get("Frequency Count", {}).get("number", 0)
            listings = [r["id"] for r in page["properties"].get("Listing", {}).get("relation", [])]
            if name:
                faqs.append({"page_id": page["id"], "name": name, "frequency": freq or 0, "listings": listings})
        if not result.get("has_more"):
            break
        start_cursor = result["next_cursor"]
    return faqs


# --------------------------------------------------------------------------- #
# Write to Notion
# --------------------------------------------------------------------------- #
def create_faq(question, category, property_notion_id):
    props = {
        "Name": {"title": [{"text": {"content": question}}]},
        "Category": {"multi_select": [{"name": category}]},
        "Frequency Count": {"number": 1},
        "Frequency Tier": {"select": {"name": "Low"}},
        "Scope": {"select": {"name": "Property-Specific"}},
        "Source": {"select": {"name": "Auto-detected"}},
        "Status": {"select": {"name": "Draft"}},
        "Last Updated": {"date": {"start": datetime.utcnow().strftime("%Y-%m-%d")}},
    }
    if property_notion_id:
        props["Listing"] = {"relation": [{"id": property_notion_id}]}
    return notion_request("POST", "/pages", {"parent": {"database_id": FAQ_DB}, "properties": props})


def update_faq(page_id, current_freq, property_notion_id, current_listings):
    props = {
        "Frequency Count": {"number": (current_freq or 0) + 1},
        "Last Updated": {"date": {"start": datetime.utcnow().strftime("%Y-%m-%d")}},
    }
    if property_notion_id and property_notion_id not in current_listings:
        props["Listing"] = {"relation": [{"id": lid} for lid in current_listings] + [{"id": property_notion_id}]}
    return notion_request("PATCH", f"/pages/{page_id}", {"properties": props})


def create_maintenance(issue, category, property_notion_id):
    props = {
        "Name": {"title": [{"text": {"content": issue}}]},
        "Category": {"select": {"name": category}},
        "Status": {"select": {"name": "New"}},
        "Priority": {"select": {"name": "Medium"}},
        "Source": {"select": {"name": "Guest Report"}},
        "Date Reported": {"date": {"start": datetime.utcnow().strftime("%Y-%m-%d")}},
    }
    if property_notion_id:
        props["Property"] = {"relation": [{"id": property_notion_id}]}
    return notion_request("POST", "/pages", {"parent": {"database_id": MAINTENANCE_DB}, "properties": props})


def create_guest_request(request_text, category, property_notion_id):
    props = {
        "Name": {"title": [{"text": {"content": request_text}}]},
        "Category": {"select": {"name": category}},
        "Status": {"select": {"name": "New"}},
    }
    if property_notion_id:
        props["Property"] = {"relation": [{"id": property_notion_id}]}
    return notion_request("POST", "/pages", {"parent": {"database_id": GUEST_REQUESTS_DB}, "properties": props})


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"=== FAQ Sync: {now} ===")

    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set")
        return

    # Step 1: Get last run time
    last_run = get_last_run()
    print(f"Last run: {last_run}")

    # Step 2: Get properties
    props_resp = hospitable_get("/properties", {"include": "listings", "per_page": "50"})
    properties = {p["id"]: p["name"] for p in props_resp.get("data", [])}
    print(f"Properties: {len(properties)}")

    # Step 3: Get property Notion IDs
    property_notion_ids = get_property_notion_ids()

    # Step 4: Get recent reservations
    thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    tomorrow = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
    all_messages = []

    for prop_uuid, prop_name in properties.items():
        try:
            res_resp = hospitable_get("/reservations", {
                "properties[]": prop_uuid,
                "include": "guest",
                "per_page": "50",
                "start_date": thirty_days_ago,
                "end_date": tomorrow,
            })
        except Exception as e:
            print(f"  Error fetching reservations for {prop_name}: {e}")
            continue

        for res in res_resp.get("data", []):
            if res.get("status") != "accepted":
                continue

            # Step 5: Get messages for this reservation
            try:
                msg_resp = hospitable_get(f"/reservations/{res['id']}/messages", {"per_page": "50"})
            except Exception as e:
                continue

            for msg in msg_resp.get("data", []):
                # Only guest messages after last_run
                sender = msg.get("sender_type", msg.get("sender", {}).get("type", ""))
                created = msg.get("created_at", "")
                body = msg.get("body", "")

                if not body or not body.strip():
                    continue
                if sender != "guest":
                    continue
                if created <= last_run:
                    continue

                # Quick pre-filter: skip very short messages (likely "ok", "thanks")
                stripped = body.strip().lower()
                if len(stripped) < 10:
                    continue
                skip_phrases = ["sounds good", "thank you", "thanks", "ok", "okay",
                                "will do", "perfect", "got it", "great", "awesome"]
                if stripped.rstrip("!.") in skip_phrases:
                    continue

                all_messages.append({
                    "body": body,
                    "property_uuid": prop_uuid,
                    "property_name": prop_name,
                    "reservation_id": res["id"],
                    "created_at": created,
                })

        time.sleep(0.3)  # rate limit

    print(f"Qualifying messages: {len(all_messages)}")

    if not all_messages:
        print("No new messages to process")
        update_last_run(now, 0)
        return

    # Step 6: Load existing FAQs
    existing_faqs = get_existing_faqs()
    print(f"Existing FAQs: {len(existing_faqs)}")

    # Step 7: Classify with Claude API (batch up to 20 at a time)
    faq_created = 0
    faq_updated = 0
    maintenance_created = 0
    request_created = 0
    skipped = 0

    for i in range(0, len(all_messages), 20):
        batch = all_messages[i:i + 20]
        try:
            classifications = claude_classify(batch, existing_faqs)
        except Exception as e:
            print(f"  Claude API error: {e}")
            continue

        for cls in classifications:
            idx = cls.get("index", 0)
            if idx >= len(batch):
                continue
            msg = batch[idx]
            msg_type = cls.get("type", "skip")
            question = cls.get("question", "")
            category = cls.get("category", "Other")
            existing_match = cls.get("existing_match")

            prop_notion_id = property_notion_ids.get(msg["property_uuid"])

            if msg_type == "faq":
                if existing_match:
                    # Find the matched FAQ and update it
                    matched = next((f for f in existing_faqs if f["name"] == existing_match), None)
                    if matched:
                        update_faq(matched["page_id"], matched["frequency"], prop_notion_id, matched["listings"])
                        matched["frequency"] += 1
                        faq_updated += 1
                        print(f"  FAQ updated: {existing_match} (+1)")
                    else:
                        create_faq(question, category, prop_notion_id)
                        faq_created += 1
                        print(f"  FAQ created: {question}")
                else:
                    create_faq(question, category, prop_notion_id)
                    existing_faqs.append({"page_id": "", "name": question, "frequency": 1, "listings": []})
                    faq_created += 1
                    print(f"  FAQ created: {question}")

            elif msg_type == "maintenance":
                create_maintenance(question, category, prop_notion_id)
                maintenance_created += 1
                print(f"  Maintenance: {question}")

            elif msg_type == "guest_request":
                create_guest_request(question, category, prop_notion_id)
                request_created += 1
                print(f"  Request: {question}")

            else:
                skipped += 1

            time.sleep(0.35)

    # Step 8: Update state
    update_last_run(now, len(all_messages))

    # Step 9: Summary
    print(f"\n=== Sync Complete ===")
    print(f"  Messages processed: {len(all_messages)}")
    print(f"  FAQs created: {faq_created}")
    print(f"  FAQs updated: {faq_updated}")
    print(f"  Maintenance items: {maintenance_created}")
    print(f"  Guest requests: {request_created}")
    print(f"  Skipped: {skipped}")


if __name__ == "__main__":
    main()
