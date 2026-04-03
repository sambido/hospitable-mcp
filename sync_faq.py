#!/usr/bin/env python3
"""
Sync guest messages from Hospitable → classify with Claude API → push to Notion.

Classifies messages into:
  - FAQ → Guest FAQs database (knowledge base / playbook)
  - Action → Action Items database (inbox for things needing human action)
  - Skip → not actionable

If an action item also matches an existing FAQ, the FAQ frequency is bumped too.

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
ACTION_ITEMS_DB = "33750c17-99cc-81d2-8fc7-c53c747abbc7"
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
   - "faq" — a question that has a standard answer and does NOT need human follow-up. The guest just needs information (amenities, parking, check-in instructions, wifi, local tips, property features). Hospitable's automated messages likely already answered it.
   - "action" — someone needs to DO something for this specific guest/reservation. This includes:
     * Requests: early check-in, late checkout, luggage drop-off, pet exception, refund, booking change
     * Issues: broken/damaged/malfunctioning items, property problems needing repair
     * Supply needs: extra towels, restocking, deliveries
     * Info gaps: guest needs information NOT covered by standard FAQ/guidebook
   - "skip" — not actionable: pure logistics ("arriving at 6pm"), confirmations ("ok thanks"), emoji-only, compliments, checkout messages, payment/billing questions handled by platform

   KEY DISTINCTION: "Where's the wifi password?" = faq (standard answer exists). "The wifi isn't working" = action (someone needs to fix it). "Can we check in early?" = action (someone needs to check the calendar and decide).

2. QUESTION: The core question or issue, distilled into a clean canonical form.
   For FAQ: "Is there a hair dryer?" not "hey do you guys happen to have a hair dryer?"
   For Action: "Early check-in request" or "Dishwasher not draining" — clear, concise, actionable

3. For "faq" type:
   - CATEGORY: Best fit from: Check-In, Check-Out, Parking & Transportation, Wifi & Tech, Kitchen, Cleaning & Laundry, Bedding & Linens, Baby & Family, Pets, Outdoor Spaces, Local Area & Dining, Property Layout, Safety & Emergencies, House Rules & Policies, Activities & Attractions
   - EXISTING_MATCH: If it semantically matches any existing FAQ name (same topic, different wording), return the exact matched name. Otherwise null.

4. For "action" type:
   - ACTION_TYPE: One of: "Request", "Issue", "Supply", "Info"
   - CATEGORY: Best fit from (comma-separated if multiple): Check-In, Check-Out, Pets, Booking & Policies, Plumbing, Electrical, HVAC, Appliances, Cleaning, Structural, Pest Control, Safety, Supplies & Restocking, Parking & Transportation, Outdoor Spaces
   - PRIORITY: One of: "Urgent" (safety hazard, no water/heat, lock failure), "High" (broken appliance, major inconvenience), "Medium" (most requests), "Low" (nice-to-have, cosmetic)
   - EXISTING_FAQ_MATCH: If the action item's topic matches an existing FAQ (e.g., early check-in request matches "Can I check in early?" FAQ), return the exact FAQ name. Otherwise null. This links the action to its playbook.

Respond with a JSON array, one object per message.

Examples:
[{"index": 0, "type": "faq", "question": "Is there parking available?", "category": "Parking & Transportation", "existing_match": "Where can I park?"}]
[{"index": 0, "type": "action", "question": "Early check-in request", "action_type": "Request", "category": "Check-In", "priority": "Medium", "existing_faq_match": "Can I check in early?"}]
[{"index": 0, "type": "action", "question": "Dishwasher not draining", "action_type": "Issue", "category": "Appliances", "priority": "High", "existing_faq_match": null}]"""

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
        "Category": {"multi_select": [{"name": c.strip()} for c in category.split(",")]},
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


def create_action_item(title, action_type, category, priority, property_notion_id,
                       msg=None, faq_page_id=None):
    """Create a row in the Action Items DB."""
    msg = msg or {}
    date_received = msg.get("created_at", "")[:10] or datetime.utcnow().strftime("%Y-%m-%d")

    props = {
        "Item": {"title": [{"text": {"content": title}}]},
        "Type": {"select": {"name": action_type}},
        "Status": {"select": {"name": "New"}},
        "Priority": {"select": {"name": priority or "Medium"}},
        "Source": {"select": {"name": "Auto-detected"}},
        "Date Received": {"date": {"start": date_received}},
    }

    # Category (multi_select, comma-separated input)
    if category:
        valid = {"Check-In", "Check-Out", "Pets", "Booking & Policies",
                 "Plumbing", "Electrical", "HVAC", "Appliances", "Cleaning",
                 "Structural", "Pest Control", "Safety", "Supplies & Restocking",
                 "Parking & Transportation", "Outdoor Spaces"}
        cats = [c.strip() for c in category.split(",") if c.strip() in valid]
        if cats:
            props["Category"] = {"multi_select": [{"name": c} for c in cats]}

    # Decision defaults: Pending for Requests, N/A for Issues/Supply/Info
    if action_type == "Request":
        props["Decision"] = {"select": {"name": "Pending"}}
    else:
        props["Decision"] = {"select": {"name": "N/A"}}

    # Guest / reservation context
    if msg.get("guest_name"):
        props["Guest Name"] = {"rich_text": [{"text": {"content": msg["guest_name"]}}]}
    if msg.get("reservation_code"):
        props["Reservation ID"] = {"rich_text": [{"text": {"content": msg["reservation_code"]}}]}
    if msg.get("body"):
        snippet = msg["body"][:200].strip()
        props["Guest Message Snippet"] = {"rich_text": [{"text": {"content": snippet}}]}

    # Property relation
    if property_notion_id:
        props["Property"] = {"relation": [{"id": property_notion_id}]}

    # FAQ link (if action item matches an existing FAQ)
    if faq_page_id:
        props["FAQ Link"] = {"relation": [{"id": faq_page_id}]}

    return notion_request("POST", "/pages", {"parent": {"database_id": ACTION_ITEMS_DB}, "properties": props})


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
            if res.get("status") not in ("accepted", "inquiry", "pending"):
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

                guest = res.get("guest", {}) or {}
                guest_name = f"{guest.get('first_name', '')} {guest.get('last_name', '')}".strip() or ""

                all_messages.append({
                    "body": body,
                    "property_uuid": prop_uuid,
                    "property_name": prop_name,
                    "reservation_id": res["id"],
                    "reservation_code": res.get("reservation_code", ""),
                    "guest_name": guest_name,
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
    action_created = 0
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

            prop_notion_id = property_notion_ids.get(msg["property_uuid"])

            if msg_type == "faq":
                existing_match = cls.get("existing_match")
                if existing_match:
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

            elif msg_type == "action":
                action_type = cls.get("action_type", "Issue")
                priority = cls.get("priority", "Medium")

                # Check if this action matches an existing FAQ (for linking)
                faq_match_name = cls.get("existing_faq_match")
                faq_page_id = None
                if faq_match_name:
                    matched_faq = next((f for f in existing_faqs if f["name"] == faq_match_name), None)
                    if matched_faq:
                        faq_page_id = matched_faq["page_id"]
                        # Also bump the FAQ frequency since this topic came up again
                        update_faq(matched_faq["page_id"], matched_faq["frequency"],
                                   prop_notion_id, matched_faq["listings"])
                        matched_faq["frequency"] += 1
                        faq_updated += 1
                        print(f"  FAQ bumped: {faq_match_name} (+1)")

                create_action_item(question, action_type, category, priority,
                                   prop_notion_id, msg, faq_page_id)
                action_created += 1
                print(f"  Action [{action_type}]: {question}")

            else:
                skipped += 1

            time.sleep(0.35)

    # Step 8: Update state
    update_last_run(now, len(all_messages))

    # Step 9: Summary
    print(f"\n=== Sync Complete ===")
    print(f"  Messages processed: {len(all_messages)}")
    print(f"  FAQs created: {faq_created}")
    print(f"  FAQs updated (incl. action bumps): {faq_updated}")
    print(f"  Action items created: {action_created}")
    print(f"  Skipped: {skipped}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import sys
        print(f"\nFATAL: {e}")
        sys.exit(1)
