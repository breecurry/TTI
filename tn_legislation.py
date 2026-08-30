import asyncio
import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup


ARCHIVE_URL = "https://wapp.capitol.tn.gov/apps/archives/default.aspx"
BASE_URL = "https://wapp.capitol.tn.gov"
USER_AGENT = (
    "The Tennessee Independent Legislative Desk/1.0 "
    "(public-interest Tennessee legislative monitoring)"
)

BILL_RE = re.compile(r"^(HB|SB)\d{4}$", re.I)
DATE_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
SCHEDULE_RE = re.compile(
    r"\b(?:for|to)\s+(\d{1,2}/\d{1,2}/\d{4})\b",
    re.I,
)


@dataclass
class BillEvent:
    action: str
    action_date_text: str
    action_date: date | None
    scheduled_for: date | None
    category: str
    event_key: str


@dataclass
class BillPage:
    bill_number: str
    general_assembly: int
    official_url: str
    caption: str | None
    page_text: str
    latest_action: str | None
    latest_action_date: date | None
    events: list[BillEvent]


def normalize_bill_number(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def parse_date(value: str) -> date | None:
    try:
        return datetime.strptime(value.strip(), "%m/%d/%Y").date()
    except Exception:
        return None


def classify_action(action: str) -> str:
    text = action.lower()

    if "vetoed by governor" in text:
        return "VETOED"
    if "signed by governor" in text:
        return "SIGNED"
    if "returned by governor without signature" in text:
        return "WITHOUT SIGNATURE"
    if "transmitted to governor" in text:
        return "TO GOVERNOR"
    if "public chapter" in text or "pub. ch." in text:
        return "ENACTED"
    if "passed senate" in text or "passed h." in text or "passed house" in text:
        return "PASSED"
    if "passed on third consideration" in text:
        return "PASSED"
    if "placed on" in text and ("calendar" in text or "cal." in text):
        return "SCHEDULED"
    if "action def" in text or "deferred" in text:
        return "DEFERRED"
    if "adopted am" in text or "amendment adopted" in text:
        return "AMENDED"
    if "filed for introduction" in text:
        return "FILED"
    return "UPDATE"


def event_hash(ga: int, bill_number: str, action: str, action_date_text: str) -> str:
    raw = f"{ga}|{bill_number}|{action_date_text}|{action}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def fetch_html(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict | None = None,
) -> tuple[str, str]:
    last_error = None

    for attempt in range(4):
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.text, str(response.url)
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(1.5 * (attempt + 1))

    raise last_error


async def discover_current_bills(
    client: httpx.AsyncClient,
) -> tuple[int, dict[str, str]]:
    html, _ = await fetch_html(client, ARCHIVE_URL)
    soup = BeautifulSoup(html, "html.parser")

    heading_text = " ".join(soup.stripped_strings)
    match = re.search(
        r"Legislation\s*-\s*(\d+)(?:th|st|nd|rd)?\s+General Assembly",
        heading_text,
        re.I,
    )
    if not match:
        match = re.search(r"Legislation\s*-\s*(\d+)(?:th|st|nd|rd)?", heading_text, re.I)

    if not match:
        raise RuntimeError("Could not determine the current Tennessee General Assembly.")

    ga = int(match.group(1))

    range_urls = []
    for a in soup.find_all("a", href=True):
        label = normalize_bill_number(a.get_text(" ", strip=True))
        href = a["href"]
        if (
            ("HB" in label or "SB" in label)
            and "BILLINDEX.ASPX" in href.upper()
            and ("STARTNUM=HB" in href.upper() or "STARTNUM=SB" in href.upper())
        ):
            range_urls.append(urljoin(BASE_URL, href))

    range_urls = list(dict.fromkeys(range_urls))
    bills: dict[str, str] = {}

    for range_url in range_urls:
        range_html, _ = await fetch_html(client, range_url)
        range_soup = BeautifulSoup(range_html, "html.parser")

        for a in range_soup.find_all("a", href=True):
            number = normalize_bill_number(a.get_text(" ", strip=True))
            if not BILL_RE.fullmatch(number):
                continue

            href = a["href"]
            if "BILLINFO" not in href.upper():
                continue

            bills[number] = urljoin(BASE_URL, href)

        await asyncio.sleep(0.10)

    return ga, bills


def _extract_caption(lines: list[str]) -> str | None:
    for line in lines:
        if " - As introduced," in line and len(line) > 40:
            return line.strip()

    for i, line in enumerate(lines):
        if line.upper().startswith("AN ACT"):
            for candidate in lines[i + 1 : i + 10]:
                if len(candidate) > 40 and not candidate.startswith("HB") and not candidate.startswith("SB"):
                    return candidate.strip()

    return None


def _extract_events(
    soup: BeautifulSoup,
    bill_number: str,
    ga: int,
) -> list[BillEvent]:
    events: list[BillEvent] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue

        header_cells = rows[0].find_all(["th", "td"])
        header_values = [
            normalize_bill_number(c.get_text(" ", strip=True))
            for c in header_cells
        ]

        if bill_number not in header_values:
            continue

        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue

            action = cells[0].get_text(" ", strip=True)
            action_date_text = cells[1].get_text(" ", strip=True)

            if not action or not DATE_RE.search(action_date_text):
                continue

            action_date = parse_date(action_date_text)

            scheduled_for = None
            schedule_match = SCHEDULE_RE.search(action)
            if schedule_match:
                scheduled_for = parse_date(schedule_match.group(1))

            events.append(
                BillEvent(
                    action=action,
                    action_date_text=action_date_text,
                    action_date=action_date,
                    scheduled_for=scheduled_for,
                    category=classify_action(action),
                    event_key=event_hash(
                        ga,
                        bill_number,
                        action,
                        action_date_text,
                    ),
                )
            )

        if events:
            break

    return events


async def fetch_bill(
    client: httpx.AsyncClient,
    bill_number: str,
    ga: int,
    official_url: str | None = None,
) -> BillPage:
    bill_number = normalize_bill_number(bill_number)

    if not BILL_RE.fullmatch(bill_number):
        raise ValueError("Use a Tennessee bill number such as HB1882 or SB1649.")

    if official_url:
        html, final_url = await fetch_html(client, official_url)
    else:
        html, final_url = await fetch_html(
            client,
            "https://wapp.capitol.tn.gov/apps/BillInfo/Default",
            params={"BillNumber": bill_number, "ga": ga},
        )

    soup = BeautifulSoup(html, "html.parser")
    lines = [x.strip() for x in soup.stripped_strings if x.strip()]
    events = _extract_events(soup, bill_number, ga)

    caption = _extract_caption(lines)
    page_text = "\n".join(lines)
    if len(page_text) > 60000:
        page_text = page_text[:60000]

    latest_action = events[0].action if events else None
    latest_action_date = events[0].action_date if events else None

    return BillPage(
        bill_number=bill_number,
        general_assembly=ga,
        official_url=final_url,
        caption=caption,
        page_text=page_text,
        latest_action=latest_action,
        latest_action_date=latest_action_date,
        events=events,
    )


def new_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=35.0,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
        limits=httpx.Limits(max_connections=4, max_keepalive_connections=4),
    )
