import asyncio
import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import urlencode

import httpx
from bs4 import BeautifulSoup


# =========================================================
# TENNESSEE LEGISLATIVE SOURCES
# =========================================================

BILLS_INDEX_URL = (
    "https://wapp.capitol.tn.gov/apps/Indexes/BillsByIndex"
)

BILL_INFO_URL = (
    "https://wapp.capitol.tn.gov/apps/BillInfo/Default"
)


# =========================================================
# GENERAL ASSEMBLY YEARS
#
# 109 = 2015-2016
# 110 = 2017-2018
# 111 = 2019-2020
# 112 = 2021-2022
# 113 = 2023-2024
# 114 = 2025-2026
#
# This automatically becomes 115 in 2027.
# =========================================================

def current_general_assembly() -> int:
    year = date.today().year

    if year < 2015:
        return 109

    return 109 + ((year - 2015) // 2)


CURRENT_GA = current_general_assembly()

# Roughly the last decade / six General Assemblies.
TRACKED_GAS = tuple(
    range(
        max(109, CURRENT_GA - 5),
        CURRENT_GA + 1,
    )
)


# =========================================================
# REGEX
# =========================================================

BILL_NUMBER_RE = re.compile(
    r"^(HB|SB)\d{4}$",
    re.IGNORECASE,
)

BILL_RANGE_RE = re.compile(
    r"\b(HB|SB)(\d{4})\s*[-–]\s*(HB|SB)(\d{4})\b",
    re.IGNORECASE,
)

SCHEDULE_DATE_RE = re.compile(
    r"\b(?:for|to)\s+(\d{1,2}/\d{1,2}/\d{4})\b",
    re.IGNORECASE,
)


# =========================================================
# DATA OBJECTS
# =========================================================

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


# =========================================================
# HELPERS
# =========================================================

def normalize_bill_number(value: str) -> str:
    return re.sub(
        r"[^A-Z0-9]",
        "",
        value.upper(),
    )


def parse_date(value: str) -> date | None:
    try:
        return datetime.strptime(
            value.strip(),
            "%m/%d/%Y",
        ).date()

    except (ValueError, TypeError):
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

    if (
        "public chapter" in text
        or "pub. ch." in text
    ):
        return "ENACTED"

    if (
        "passed senate" in text
        or "passed house" in text
        or "passed h." in text
        or "passed on third consideration" in text
    ):
        return "PASSED"

    if (
        "placed on" in text
        and (
            "calendar" in text
            or "cal." in text
        )
    ):
        return "SCHEDULED"

    if (
        "action def" in text
        or "deferred" in text
    ):
        return "DEFERRED"

    if (
        "adopted am" in text
        or "amendment adopted" in text
    ):
        return "AMENDED"

    if "filed for introduction" in text:
        return "FILED"

    return "UPDATE"


def make_event_key(
    general_assembly: int,
    bill_number: str,
    action: str,
    action_date_text: str,
) -> str:

    raw = (
        f"{general_assembly}|"
        f"{bill_number}|"
        f"{action_date_text}|"
        f"{action}"
    )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()


def build_bill_url(
    bill_number: str,
    general_assembly: int,
) -> str:

    query = urlencode(
        {
            "BillNumber": bill_number,
            "ga": general_assembly,
        }
    )

    return f"{BILL_INFO_URL}?{query}"


# =========================================================
# HTTP
# =========================================================

def new_client() -> httpx.AsyncClient:

    return httpx.AsyncClient(
        timeout=40.0,
        follow_redirects=True,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 "
                "(KHTML, like Gecko) Version/18.0 Safari/605.1.15"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
        limits=httpx.Limits(
            max_connections=4,
            max_keepalive_connections=4,
        ),
    )


async def fetch_html(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict | None = None,
) -> tuple[str, str]:

    last_error = None

    for attempt in range(4):

        try:
            response = await client.get(
                url,
                params=params,
            )

            response.raise_for_status()

            return (
                response.text,
                str(response.url),
            )

        except Exception as exc:

            last_error = exc

            await asyncio.sleep(
                1.5 * (attempt + 1)
            )

    raise last_error


# =========================================================
# BILL DISCOVERY
# =========================================================

async def discover_bills_for_ga(
    client: httpx.AsyncClient,
    general_assembly: int,
) -> dict[str, str]:

    html, final_url = await fetch_html(
        client,
        BILLS_INDEX_URL,
        params={
            "ga": general_assembly,
        },
    )

    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    page_text = soup.get_text(
        " ",
        strip=True,
    )

    # Make sure Tennessee actually returned the requested
    # General Assembly instead of an error/login/blank page.

    expected_heading = re.compile(
        rf"\b{general_assembly}"
        rf"(?:st|nd|rd|th)?\s+"
        rf"General Assembly\b",
        re.IGNORECASE,
    )

    if not expected_heading.search(page_text):

        raise RuntimeError(
            f"Tennessee did not return the "
            f"{general_assembly}th General Assembly "
            f"bill index. URL returned: {final_url}"
        )

    ranges = BILL_RANGE_RE.findall(
        page_text
    )

    if not ranges:

        raise RuntimeError(
            f"Tennessee returned the "
            f"{general_assembly}th General Assembly page, "
            f"but no House or Senate bill ranges were found."
        )

    bills: dict[str, str] = {}

    for (
        start_prefix,
        start_number,
        end_prefix,
        end_number,
    ) in ranges:

        start_prefix = start_prefix.upper()
        end_prefix = end_prefix.upper()

        # Only regular HB and SB legislation.
        if start_prefix not in {"HB", "SB"}:
            continue

        if end_prefix != start_prefix:
            continue

        start = int(start_number)
        end = int(end_number)

        if end < start:
            continue

        for number in range(
            start,
            end + 1,
        ):

            bill_number = (
                f"{start_prefix}{number:04d}"
            )

            bills[bill_number] = build_bill_url(
                bill_number,
                general_assembly,
            )

    if not bills:

        raise RuntimeError(
            f"No HB/SB bills could be generated for "
            f"the {general_assembly}th General Assembly."
        )

    return bills


async def discover_current_bills(
    client: httpx.AsyncClient,
) -> tuple[int, dict[str, str]]:

    # IMPORTANT:
    # We do NOT try to read/guess the General Assembly
    # from Tennessee's webpage anymore.
    #
    # In 2025-2026 this is explicitly 114.

    general_assembly = CURRENT_GA

    bills = await discover_bills_for_ga(
        client,
        general_assembly,
    )

    return (
        general_assembly,
        bills,
    )


# =========================================================
# BILL PAGE PARSING
# =========================================================

def extract_caption(
    soup: BeautifulSoup,
) -> str | None:

    lines = [
        text.strip()
        for text
        in soup.stripped_strings
        if text.strip()
    ]

    # Tennessee normally displays:
    #
    # "State Universities - As introduced, ..."
    #
    # This is the useful public-facing caption.

    for line in lines:

        if (
            " - As introduced," in line
            and len(line) > 30
        ):
            return line

    # Fallback for pages formatted slightly differently.

    for index, line in enumerate(lines):

        if line.upper().startswith(
            "AN ACT"
        ):

            candidates = lines[
                index + 1:
                index + 10
            ]

            for candidate in candidates:

                if (
                    len(candidate) > 30
                    and not candidate.startswith("HB")
                    and not candidate.startswith("SB")
                ):
                    return candidate

    return None


def extract_history(
    soup: BeautifulSoup,
    bill_number: str,
    general_assembly: int,
) -> list[BillEvent]:

    bill_number = normalize_bill_number(
        bill_number
    )

    events: list[BillEvent] = []

    for table in soup.find_all(
        "table"
    ):

        rows = table.find_all(
            "tr"
        )

        if not rows:
            continue

        first_row_cells = rows[0].find_all(
            ["th", "td"]
        )

        if not first_row_cells:
            continue

        first_header = normalize_bill_number(
            first_row_cells[0].get_text(
                " ",
                strip=True,
            )
        )

        # This prevents accidentally reading the
        # companion bill's history table.

        if first_header != bill_number:
            continue

        for row in rows[1:]:

            cells = row.find_all(
                ["td", "th"]
            )

            if len(cells) < 2:
                continue

            action = cells[0].get_text(
                " ",
                strip=True,
            )

            action_date_text = (
                cells[1]
                .get_text(
                    " ",
                    strip=True,
                )
            )

            if not action:
                continue

            action_date = parse_date(
                action_date_text
            )

            # Skip rows that are not actual dated
            # legislative history events.

            if action_date is None:
                continue

            scheduled_for = None

            schedule_match = (
                SCHEDULE_DATE_RE.search(
                    action
                )
            )

            if schedule_match:

                scheduled_for = parse_date(
                    schedule_match.group(1)
                )

            events.append(
                BillEvent(
                    action=action,
                    action_date_text=action_date_text,
                    action_date=action_date,
                    scheduled_for=scheduled_for,
                    category=classify_action(
                        action
                    ),
                    event_key=make_event_key(
                        general_assembly,
                        bill_number,
                        action,
                        action_date_text,
                    ),
                )
            )

        if events:
            break

    return events


# =========================================================
# FETCH ONE BILL
# =========================================================

async def fetch_bill(
    client: httpx.AsyncClient,
    bill_number: str,
    general_assembly: int,
    official_url: str | None = None,
) -> BillPage:

    bill_number = normalize_bill_number(
        bill_number
    )

    if not BILL_NUMBER_RE.fullmatch(
        bill_number
    ):

        raise ValueError(
            "Use a Tennessee bill number "
            "such as HB1882 or SB1649."
        )

    url = (
        official_url
        or build_bill_url(
            bill_number,
            general_assembly,
        )
    )

    html, final_url = await fetch_html(
        client,
        url,
    )

    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    caption = extract_caption(
        soup
    )

    events = extract_history(
        soup,
        bill_number,
        general_assembly,
    )

    page_text = soup.get_text(
        "\n",
        strip=True,
    )

    # Plenty for search/AI context without shoving
    # absurd amounts of HTML-derived text into Postgres.

    if len(page_text) > 60000:
        page_text = page_text[:60000]

    latest_action = None
    latest_action_date = None

    if events:

        # Tennessee displays newest history first.

        latest_action = (
            events[0].action
        )

        latest_action_date = (
            events[0].action_date
        )

    return BillPage(
        bill_number=bill_number,
        general_assembly=general_assembly,
        official_url=final_url,
        caption=caption,
        page_text=page_text,
        latest_action=latest_action,
        latest_action_date=latest_action_date,
        events=events,
    )
