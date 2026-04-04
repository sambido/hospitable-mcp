#!/usr/bin/env python3
"""
Orphan Gap Detector — Auto-fix min-stay for unbookable gaps.

PriceLabs dynamic min-stay doesn't always account for gaps created by
manual bookings or blocks. This script detects gaps where the min_stay
exceeds the gap length (making them unbookable) and pushes PriceLabs
overrides to lower the min_stay to match the gap length.

Runs daily via GitHub Actions.
"""

import json, os, ssl, time, urllib.request, urllib.error
from datetime import datetime, timedelta

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
HOSPITABLE_PAT = os.environ.get("HOSPITABLE_PAT", "")
PRICELABS_API_KEY = os.environ.get("PRICELABS_API_KEY", "")

if not HOSPITABLE_PAT:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
        HOSPITABLE_PAT = HOSPITABLE_PAT or os.environ.get("HOSPITABLE_PAT", "")
    except ImportError:
        pass

CTX = ssl.create_default_context()

# Max gap size to fix (nights). Gaps longer than this aren't orphans.
MAX_GAP_NIGHTS = 5

# How far out to scan (days)
SCAN_DAYS = 270

# All properties with both Hospitable UUID and PriceLabs ID
# PMS is always "smartbnb" for PriceLabs
PROPERTIES = {
    "92a1c198-4d3e-4d1b-a5f8-e90f98f1c49c": {"name": "65th", "pricelabs_id": "1047936"},
    "f3fd4981-3f21-4c5a-8888-ba259834ddb5": {"name": "8th", "pricelabs_id": "1104382"},
    "4dbf5125-6efe-4097-90f6-3fab87a911d2": {"name": "Bridget", "pricelabs_id": "4dbf5125-6efe-4097-90f6-3fab87a911d2"},
    "725901a7-f4a4-4892-866b-df14dc8f4ff7": {"name": "32nd", "pricelabs_id": "1047934"},
    "123ee545-ddf9-4e25-b6d0-e597afc5612b": {"name": "Jeremy", "pricelabs_id": "123ee545-ddf9-4e25-b6d0-e597afc5612b"},
    "f1970a87-2c41-4cd8-b222-329980b45a78": {"name": "Mary Anne", "pricelabs_id": "1047942"},
    "a8cd20bc-16f9-44d0-8c3f-12bea51720cb": {"name": "Nordic Loft", "pricelabs_id": "a8cd20bc-16f9-44d0-8c3f-12bea51720cb"},
    "c50f431b-1d44-40fd-8788-92708710a1cc": {"name": "Andy", "pricelabs_id": "1047944"},
    "d708140c-4ba0-4673-ba44-0b11d4f97181": {"name": "Lia", "pricelabs_id": "1558876"},
    "8423a091-1513-4d98-9e68-c6c3888b1f9e": {"name": "Michael", "pricelabs_id": "8423a091-1513-4d98-9e68-c6c3888b1f9e"},
    "bef6a386-1446-4c09-a7db-757824cd6d35": {"name": "Eve", "pricelabs_id": "bef6a386-1446-4c09-a7db-757824cd6d35"},
    "bd0528ad-c1cb-4035-821a-fb1199dfacaa": {"name": "Chris", "pricelabs_id": "1374298"},
    "13a74151-c6bc-434b-8de1-549f048d77c7": {"name": "Gunny", "pricelabs_id": "1047938"},
    "c80e149c-0ae3-4cf1-965b-5fd12e97f7f6": {"name": "Sophia", "pricelabs_id": "c80e149c-0ae3-4cf1-965b-5fd12e97f7f6"},
    "ab7b6a1b-b731-4046-8406-654a3b62b2cb": {"name": "Assim", "pricelabs_id": "ab7b6a1b-b731-4046-8406-654a3b62b2cb"},
    "10bd7b2b-e250-416f-b45f-a1a4d0e92e3c": {"name": "Susan", "pricelabs_id": "10bd7b2b-e250-416f-b45f-a1a4d0e92e3c"},
    "eefb5918-5149-4b4e-bdd0-277754409cb0": {"name": "Chad", "pricelabs_id": "eefb5918-5149-4b4e-bdd0-277754409cb0"},
    "56ea4fe3-3445-4a6b-962f-a02cbbd2869b": {"name": "Matthew", "pricelabs_id": "56ea4fe3-3445-4a6b-962f-a02cbbd2869b"},
    "5cf63104-6ae7-40b2-aa7d-c10d18822ccd": {"name": "Don and Kathy", "pricelabs_id": "5cf63104-6ae7-40b2-aa7d-c10d18822ccd"},
    "9bfda321-b0f0-4c4e-8f03-eeb86ef3c87f": {"name": "Sundee", "pricelabs_id": "9bfda321-b0f0-4c4e-8f03-eeb86ef3c87f"},
    "14912b54-f5e0-47ac-a8c2-1e1d9e17bbd6": {"name": "Adam", "pricelabs_id": "14912b54-f5e0-47ac-a8c2-1e1d9e17bbd6"},
    "c84923ff-a37b-4463-93d6-d192de05be78": {"name": "Danial", "pricelabs_id": "c84923ff-a37b-4463-93d6-d192de05be78"},
    "5a3010a8-602d-4d79-9fa0-18f99d02fb88": {"name": "Dara", "pricelabs_id": "5a3010a8-602d-4d79-9fa0-18f99d02fb88"},
    "df375ad6-b2e8-43de-a7f2-45d658864736": {"name": "Miller Bay", "pricelabs_id": "df375ad6-b2e8-43de-a7f2-45d658864736"},
    "d92cdc13-8bd9-4803-a277-55f8ba36bd29": {"name": "Palatine", "pricelabs_id": "d92cdc13-8bd9-4803-a277-55f8ba36bd29"},
}

PMS = "smartbnb"


# --------------------------------------------------------------------------- #
# API helpers
# --------------------------------------------------------------------------- #
def hospitable_calendar(property_uuid, start_date, end_date):
    """Pull calendar from Hospitable API. Returns list of day objects."""
    url = (
        f"https://public.api.hospitable.com/v2/properties/{property_uuid}/calendar"
        f"?start_date={start_date}&end_date={end_date}"
    )
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {HOSPITABLE_PAT}",
        "Accept": "application/json",
    })
    try:
        resp = urllib.request.urlopen(req, context=CTX)
        data = json.loads(resp.read())
        return data.get("data", {}).get("days", [])
    except urllib.error.HTTPError as e:
        print(f"    Hospitable error {e.code}: {e.read().decode()[:200]}")
        return []
    except Exception as e:
        print(f"    Hospitable error: {e}")
        return []


def pricelabs_get_overrides(listing_id):
    """Get existing PriceLabs overrides for a listing."""
    url = (
        f"https://api.pricelabs.co/v1/listing/{listing_id}/overrides"
        f"?pms={PMS}"
    )
    req = urllib.request.Request(url, headers={
        "X-API-Key": PRICELABS_API_KEY,
        "Accept": "application/json",
    })
    try:
        resp = urllib.request.urlopen(req, context=CTX)
        return json.loads(resp.read())
    except Exception as e:
        print(f"    PriceLabs get_overrides error: {e}")
        return {}


def pricelabs_set_overrides(listing_id, overrides):
    """Push min_stay overrides to PriceLabs."""
    url = f"https://api.pricelabs.co/v1/listing/{listing_id}/overrides?pms={PMS}"
    body = json.dumps(overrides).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "X-API-Key": PRICELABS_API_KEY,
        "Content-Type": "application/json",
    })
    try:
        resp = urllib.request.urlopen(req, context=CTX)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"    PriceLabs set_overrides error {e.code}: {e.read().decode()[:200]}")
        return None


# --------------------------------------------------------------------------- #
# Gap detection
# --------------------------------------------------------------------------- #
def find_orphan_gaps(days):
    """
    Walk calendar days and find gaps of available dates bounded by
    reserved/blocked dates. Returns list of gaps, each being a list of
    day dicts with dates and min_stay values.
    """
    gaps = []
    current_gap = []

    for day in days:
        available = day.get("status", {}).get("available", True)
        date = day.get("date", "")
        min_stay = day.get("min_stay") or 1

        if available:
            current_gap.append({"date": date, "min_stay": min_stay})
        else:
            if current_gap:
                gaps.append(current_gap)
                current_gap = []

    # Don't add trailing available dates — they're not bounded by a booking on the right
    # (they extend into open calendar, not a true orphan gap)

    return gaps


def needs_fix(gap):
    """
    Check if a gap needs a min_stay fix.
    Returns True if any date in the gap has min_stay > gap length.
    """
    gap_length = len(gap)
    return any(d["min_stay"] > gap_length for d in gap)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    end_date = (today + timedelta(days=SCAN_DAYS)).strftime("%Y-%m-%d")

    print(f"=== Orphan Gap Detector: {today_str} ===")
    print(f"Scanning {SCAN_DAYS} days across {len(PROPERTIES)} properties")
    print(f"Max gap size: {MAX_GAP_NIGHTS} nights\n")

    total_gaps_found = 0
    total_dates_fixed = 0
    dry_run = not PRICELABS_API_KEY
    if dry_run:
        print("[DRY RUN — no PRICELABS_API_KEY set, will not push overrides]\n")

    for hosp_uuid, prop in PROPERTIES.items():
        name = prop["name"]
        pl_id = prop["pricelabs_id"]
        print(f"--- {name} ---")

        # Pull calendar in 90-day chunks
        all_days = []
        chunk_start = today
        while chunk_start < datetime.strptime(end_date, "%Y-%m-%d"):
            chunk_end = min(chunk_start + timedelta(days=89),
                           datetime.strptime(end_date, "%Y-%m-%d"))
            days = hospitable_calendar(
                hosp_uuid,
                chunk_start.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"),
            )
            all_days.extend(days)
            chunk_start = chunk_end + timedelta(days=1)
            time.sleep(0.3)

        if not all_days:
            print(f"  No calendar data")
            continue

        print(f"  {len(all_days)} days loaded")

        # Get existing PriceLabs overrides to avoid clobbering manual ones
        existing_overrides = {}
        if not dry_run:
            override_data = pricelabs_get_overrides(pl_id)
            for ov in override_data.get("overrides", []):
                if ov.get("min_stay") is not None:
                    existing_overrides[ov["date"]] = ov["min_stay"]

        # Find gaps
        gaps = find_orphan_gaps(all_days)
        orphan_gaps = [g for g in gaps if len(g) <= MAX_GAP_NIGHTS and needs_fix(g)]

        if not orphan_gaps:
            print(f"  No orphan gaps found")
            continue

        print(f"  Found {len(orphan_gaps)} orphan gap(s)")
        total_gaps_found += len(orphan_gaps)

        # Build overrides
        overrides_to_push = []
        for gap in orphan_gaps:
            gap_length = len(gap)
            dates = [d["date"] for d in gap]
            current_mins = [d["min_stay"] for d in gap]
            print(f"  Gap: {dates[0]} to {dates[-1]} ({gap_length} nights, current min_stay: {current_mins})")

            for d in gap:
                if d["min_stay"] <= gap_length:
                    continue  # Already bookable
                if d["date"] in existing_overrides:
                    print(f"    Skip {d['date']}: has existing manual override (min_stay={existing_overrides[d['date']]})")
                    continue
                overrides_to_push.append({
                    "date": d["date"],
                    "min_stay": gap_length,
                })
                print(f"    {d['date']}: min_stay {d['min_stay']} -> {gap_length}")

        if not overrides_to_push:
            print(f"  No overrides needed (already bookable or has manual overrides)")
            continue

        total_dates_fixed += len(overrides_to_push)

        if dry_run:
            print(f"  [DRY RUN] Would push {len(overrides_to_push)} override(s)")
        else:
            result = pricelabs_set_overrides(pl_id, overrides_to_push)
            if result:
                print(f"  Pushed {len(overrides_to_push)} override(s) to PriceLabs")
            else:
                print(f"  ERROR pushing overrides")

        time.sleep(0.5)

    print(f"\n=== Summary ===")
    print(f"Properties scanned: {len(PROPERTIES)}")
    print(f"Orphan gaps found: {total_gaps_found}")
    print(f"Dates fixed: {total_dates_fixed}")
    if dry_run:
        print("(Dry run — no changes pushed)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import sys, traceback
        print(f"\nFATAL: {e}")
        traceback.print_exc()
        sys.exit(1)
