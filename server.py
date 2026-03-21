"""
Hospitable MCP Server for Breezy Beach Stays
=============================================
Dual-transport MCP server connecting Claude to Hospitable API v2.
Supports STDIO (Claude Desktop) and Streamable HTTP (claude.ai custom connector).

Setup:
  1. Get your PAT from my.hospitable.com > Apps > API access > Access tokens
  2. Set HOSPITABLE_PAT environment variable
  3. See README.md for Claude Desktop and claude.ai configuration

Author: Mike @ Breezy Beach Stays
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import httpx
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_BASE = "https://public.api.hospitable.com/v2"
PAT = os.environ.get("HOSPITABLE_PAT", "")
MCP_API_KEY = os.environ.get("MCP_API_KEY", "")
DEFAULT_TIMEOUT = 30.0
PER_PAGE = 50

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hospitable-mcp")

# ---------------------------------------------------------------------------
# Determine transport mode from CLI args
# ---------------------------------------------------------------------------
# Usage:
#   python server.py          -> STDIO (default, for Claude Desktop)
#   python server.py --http   -> Streamable HTTP on port 8000 (for claude.ai)
#   python server.py --http --port 9000  -> HTTP on custom port

HTTP_MODE = "--http" in sys.argv
# Railway (and most PaaS) set PORT in the environment; fall back to --port arg or 8000
HTTP_PORT = int(os.environ.get("PORT", 8000))
if "--port" in sys.argv:
    try:
        HTTP_PORT = int(sys.argv[sys.argv.index("--port") + 1])
    except (IndexError, ValueError):
        pass

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "Hospitable",
    instructions=(
        "Access Hospitable vacation rental data for Breezy Beach Stays: "
        "properties, reservations, calendar, guest messaging, and reviews."
    ),
    stateless_http=HTTP_MODE,
    json_response=True,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    if not PAT:
        raise ValueError(
            "HOSPITABLE_PAT environment variable is not set. "
            "Get your token from my.hospitable.com > Apps > API access."
        )
    return {
        "Authorization": f"Bearer {PAT}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


async def _get(path: str, params: dict | None = None) -> dict:
    """Make an authenticated GET request to the Hospitable API."""
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.get(
            f"{API_BASE}{path}",
            headers=_headers(),
            params=params or {},
        )
        resp.raise_for_status()
        return resp.json()


async def _post(path: str, body: dict) -> dict:
    """Make an authenticated POST request to the Hospitable API."""
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.post(
            f"{API_BASE}{path}",
            headers=_headers(),
            json=body,
        )
        resp.raise_for_status()
        return resp.json()


async def _put(path: str, body: dict) -> dict:
    """Make an authenticated PUT request to the Hospitable API."""
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.put(
            f"{API_BASE}{path}",
            headers=_headers(),
            json=body,
        )
        resp.raise_for_status()
        return resp.json()


def _fmt(data: dict) -> str:
    """Format API response as readable JSON."""
    return json.dumps(data, indent=2, default=str)


# ---------------------------------------------------------------------------
# Tools - Properties
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_properties(
    include: str = "listings",
    page: int = 1,
    per_page: int = 50,
) -> str:
    """
    List all Breezy Beach Stays properties in Hospitable.

    Args:
        include: Comma-separated related data to include. Options: listings, details
        page: Page number for pagination (default 1)
        per_page: Results per page, max 50 (default 50)

    Returns property UUID, name, address, and listing details.
    """
    data = await _get("/properties", {
        "include": include,
        "page": page,
        "per_page": per_page,
    })
    return _fmt(data)


@mcp.tool()
async def get_property(
    property_uuid: str,
    include: str = "listings,details",
) -> str:
    """
    Get detailed info for a specific property.

    Args:
        property_uuid: The UUID of the property
        include: Comma-separated related data. Options: listings, details
    """
    data = await _get(f"/properties/{property_uuid}", {
        "include": include,
    })
    return _fmt(data)


@mcp.tool()
async def search_properties(
    start_date: str,
    end_date: str,
    adults: int = 2,
    children: int = 0,
) -> str:
    """
    Search for available properties in a date range.

    Args:
        start_date: Check-in date (YYYY-MM-DD)
        end_date: Check-out date (YYYY-MM-DD)
        adults: Number of adults (default 2)
        children: Number of children (default 0)
    """
    data = await _get("/properties/search", {
        "start_date": start_date,
        "end_date": end_date,
        "adults": adults,
        "children": children,
    })
    return _fmt(data)


# ---------------------------------------------------------------------------
# Tools - Reservations
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_reservations(
    property_uuids: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include: str = "guest,properties",
    page: int = 1,
    per_page: int = 50,
) -> str:
    """
    List reservations for one or more properties.

    IMPORTANT: The Hospitable API requires at least one property UUID.
    Use list_properties first to get UUIDs if needed.

    Args:
        property_uuids: Comma-separated property UUIDs (REQUIRED by Hospitable API)
        start_date: Filter by check-in date on or after (YYYY-MM-DD)
        end_date: Filter by check-out date on or before (YYYY-MM-DD)
        include: Related data. Options: guest, properties, financials
        page: Page number (default 1)
        per_page: Results per page, max 50 (default 50)
    """
    params: dict = {
        "include": include,
        "page": page,
        "per_page": per_page,
    }
    # Hospitable requires properties[] as array param
    uuids = [u.strip() for u in property_uuids.split(",")]
    for uuid in uuids:
        params.setdefault("properties[]", [])
        if isinstance(params["properties[]"], list):
            params["properties[]"].append(uuid)

    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    # httpx needs special handling for array params
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        query_parts = []
        for uuid in uuids:
            query_parts.append(f"properties[]={uuid}")
        query_parts.append(f"include={include}")
        query_parts.append(f"page={page}")
        query_parts.append(f"per_page={per_page}")
        if start_date:
            query_parts.append(f"start_date={start_date}")
        if end_date:
            query_parts.append(f"end_date={end_date}")

        url = f"{API_BASE}/reservations?{'&'.join(query_parts)}"
        resp = await client.get(url, headers=_headers())
        resp.raise_for_status()
        return _fmt(resp.json())


@mcp.tool()
async def get_reservation(
    reservation_uuid: str,
    include: str = "guest,properties,financials",
) -> str:
    """
    Get full details for a specific reservation including financials.

    Args:
        reservation_uuid: The UUID of the reservation
        include: Related data. Options: guest, properties, financials
    """
    data = await _get(f"/reservations/{reservation_uuid}", {
        "include": include,
    })
    return _fmt(data)


@mcp.tool()
async def get_upcoming_checkins(
    days_ahead: int = 3,
) -> str:
    """
    Get all reservations checking in within the next N days across all properties.
    Fetches properties first, then queries reservations.

    Args:
        days_ahead: Number of days to look ahead (default 3)
    """
    # Step 1: Get all property UUIDs
    props = await _get("/properties", {"per_page": 50})
    uuids = [p["id"] for p in props.get("data", [])]

    if not uuids:
        return json.dumps({"message": "No properties found"})

    # Step 2: Query reservations
    today = datetime.now().strftime("%Y-%m-%d")
    future = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    query_parts = []
    for uuid in uuids:
        query_parts.append(f"properties[]={uuid}")
    query_parts.append("include=guest,properties")
    query_parts.append(f"start_date={today}")
    query_parts.append(f"end_date={future}")
    query_parts.append("per_page=50")

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        url = f"{API_BASE}/reservations?{'&'.join(query_parts)}"
        resp = await client.get(url, headers=_headers())
        resp.raise_for_status()
        data = resp.json()

    # Filter to actual check-ins in the window
    checkins = []
    for r in data.get("data", []):
        checkin = r.get("check_in") or r.get("start_date", "")
        if today <= checkin <= future:
            checkins.append(r)

    return _fmt({
        "check_ins": checkins,
        "count": len(checkins),
        "window": f"{today} to {future}",
    })


@mcp.tool()
async def get_upcoming_checkouts(
    days_ahead: int = 3,
) -> str:
    """
    Get all reservations checking out within the next N days across all properties.

    Args:
        days_ahead: Number of days to look ahead (default 3)
    """
    props = await _get("/properties", {"per_page": 50})
    uuids = [p["id"] for p in props.get("data", [])]

    if not uuids:
        return json.dumps({"message": "No properties found"})

    today = datetime.now().strftime("%Y-%m-%d")
    future = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    # Need a wider window to catch reservations whose checkout falls in range
    lookback = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    query_parts = []
    for uuid in uuids:
        query_parts.append(f"properties[]={uuid}")
    query_parts.append("include=guest,properties")
    query_parts.append(f"start_date={lookback}")
    query_parts.append(f"end_date={future}")
    query_parts.append("per_page=50")

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        url = f"{API_BASE}/reservations?{'&'.join(query_parts)}"
        resp = await client.get(url, headers=_headers())
        resp.raise_for_status()
        data = resp.json()

    checkouts = []
    for r in data.get("data", []):
        checkout = r.get("check_out") or r.get("end_date", "")
        if today <= checkout <= future:
            checkouts.append(r)

    return _fmt({
        "check_outs": checkouts,
        "count": len(checkouts),
        "window": f"{today} to {future}",
    })


# ---------------------------------------------------------------------------
# Tools - Calendar
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_calendar(
    property_uuid: str,
    start_date: str,
    end_date: str,
) -> str:
    """
    Get calendar data (pricing, availability, min stay) for a property.

    Args:
        property_uuid: The UUID of the property
        start_date: Start of date range (YYYY-MM-DD)
        end_date: End of date range (YYYY-MM-DD)
    """
    data = await _get(f"/properties/{property_uuid}/calendar", {
        "start_date": start_date,
        "end_date": end_date,
    })
    return _fmt(data)


@mcp.tool()
async def update_calendar(
    property_uuid: str,
    updates_json: str,
) -> str:
    """
    Update calendar pricing, availability, or minimum stay for specific dates.
    Rate limited: 1000 requests/minute.

    Args:
        property_uuid: The UUID of the property
        updates_json: JSON string of updates array. Each item needs:
            - date: "YYYY-MM-DD"
            - price: {"amount": 15000} (amount in CENTS, so $150 = 15000)
            - available: true/false (optional)
            - min_stay: integer (optional)

    Example updates_json:
        [{"date": "2026-07-04", "price": {"amount": 30000}, "available": true, "min_stay": 3}]
    """
    try:
        updates = json.loads(updates_json)
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Invalid JSON: {e}"})

    data = await _put(f"/properties/{property_uuid}/calendar", updates)
    return _fmt(data)


# ---------------------------------------------------------------------------
# Tools - Messages
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_messages(
    reservation_uuid: str,
    page: int = 1,
    per_page: int = 50,
) -> str:
    """
    Get the guest conversation thread for a reservation.

    Note: Messages with no sender field are AI-generated (from Hospitable automation).
    Messages with a sender are from the guest or from a team member.

    Args:
        reservation_uuid: The UUID of the reservation
        page: Page number (default 1)
        per_page: Results per page (default 50)
    """
    data = await _get(f"/reservations/{reservation_uuid}/messages", {
        "page": page,
        "per_page": per_page,
    })
    return _fmt(data)


@mcp.tool()
async def send_message(
    reservation_uuid: str,
    body: str,
    images: Optional[str] = None,
) -> str:
    """
    Send a message to a guest for a specific reservation.
    Rate limited: 2/minute per reservation, 50/5 minutes globally.

    The message will be delivered through the booking platform (Airbnb, Vrbo, etc).

    Args:
        reservation_uuid: The UUID of the reservation
        body: Message text to send
        images: Optional comma-separated image URLs to attach
    """
    payload: dict = {"body": body}
    if images:
        payload["images"] = [url.strip() for url in images.split(",")]

    data = await _post(f"/reservations/{reservation_uuid}/messages", payload)
    return _fmt(data)


# ---------------------------------------------------------------------------
# Tools - Reviews
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_reviews(
    property_uuid: str,
    include: str = "guest",
    page: int = 1,
    per_page: int = 50,
) -> str:
    """
    Get reviews for a specific property.

    Args:
        property_uuid: The UUID of the property
        include: Related data to include. Options: guest
        page: Page number (default 1)
        per_page: Results per page (default 50)
    """
    data = await _get(f"/properties/{property_uuid}/reviews", {
        "include": include,
        "page": page,
        "per_page": per_page,
    })
    return _fmt(data)


@mcp.tool()
async def respond_to_review(
    review_uuid: str,
    response: str,
) -> str:
    """
    Post a public response to a guest review.
    The response will be visible on the listing platform (Airbnb, Vrbo, etc).

    Args:
        review_uuid: The UUID of the review
        response: Your public response text
    """
    data = await _post(f"/reviews/{review_uuid}/response", {
        "response": response,
    })
    return _fmt(data)


# ---------------------------------------------------------------------------
# Tools - User / Account
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_account_info() -> str:
    """
    Get the authenticated Hospitable user/account info.
    Returns name, email, and account details.
    Useful for verifying the API connection is working.
    """
    data = await _get("/user")
    return _fmt(data)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

class APIKeyMiddleware(BaseHTTPMiddleware):
    """Require a Bearer token matching MCP_API_KEY on all HTTP requests."""
    async def dispatch(self, request, call_next):
        if not MCP_API_KEY:
            # No key configured — allow all (useful for local testing)
            return await call_next(request)
        auth = request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip()
        if token != MCP_API_KEY:
            return Response("Unauthorized", status_code=401)
        return await call_next(request)


if __name__ == "__main__":
    if HTTP_MODE:
        logger.info(f"Starting Hospitable MCP server in HTTP mode on port {HTTP_PORT}")
        if MCP_API_KEY:
            logger.info("API key authentication enabled")
        else:
            logger.warning("MCP_API_KEY not set — running without authentication")
        app = mcp.streamable_http_app()
        app.add_middleware(APIKeyMiddleware)
        uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT)
    else:
        logger.info("Starting Hospitable MCP server in STDIO mode")
        mcp.run()
