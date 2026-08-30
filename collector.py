import hashlib
import re
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup


TN_BILL_URL = "https://wapp.capitol.tn.gov/apps/BillInfo/Default"


def normalize_bill_number(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def make_event_key(
    general_assembly: int,
    bill_number: str,
    action: str,
    action_date: str,
) -> str:
    raw = (
        f"{general_assembly}|{bill_number}|"
        f"{action_date}|{action}"
    )

    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_tn_date(value: str):
    value = value.strip()

    try:
        parsed = datetime.strptime(value, "%m/%d/%Y")
        return parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


async def fetch_bill_page(
    bill_number: str,
    general_assembly: int,
):
    bill_number = normalize_bill_number(bill_number)

    params = {
        "BillNumber": bill_number,
        "ga": general_assembly,
    }

    headers = {
        "User-Agent": (
            "The Tennessee Independent Legislative Desk "
            "(legislative research bot)"
        )
    }

    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers=headers,
    ) as client:
        response = await client.get(
            TN_BILL_URL,
            params=params,
        )

        response.raise_for_status()

    return response.text, str(response.url)


def extract_caption(soup: BeautifulSoup):
    lines = [
        text.strip()
        for text in soup.stripped_strings
        if text.strip()
    ]

    act_text = None
    description = None

    for index, line in enumerate(lines):
        if line.upper().startswith("AN ACT"):
            act_text = line

            for possible in lines[index + 1:index + 8]:
                if (
                    "As introduced" in possible
                    or "Amends TCA" in possible
                    or "TCA Title" in possible
                ):
                    description = possible
                    break

            break

    return act_text, description


def extract_history(
    soup: BeautifulSoup,
    requested_bill: str,
):
    requested_bill = normalize_bill_number(
        requested_bill
    )

    events = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")

        if not rows:
            continue

        first_cells = rows[0].find_all(
            ["th", "td"]
        )

        if not first_cells:
            continue

        first_value = normalize_bill_number(
            first_cells[0].get_text(
                " ",
                strip=True,
            )
        )

        if first_value != requested_bill:
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

            date_text = cells[1].get_text(
                " ",
                strip=True,
            )

            if not action:
                continue

            events.append(
                {
                    "action": action,
                    "date_text": date_text,
                    "action_date": parse_tn_date(
                        date_text
                    ),
                }
            )

        if events:
            break

    return events


async def sync_bill(
    pool,
    bill_number: str,
    general_assembly: int = 114,
):
    bill_number = normalize_bill_number(
        bill_number
    )

    if not re.fullmatch(
        r"(HB|SB)\d+",
        bill_number,
    ):
        raise ValueError(
            "Use a Tennessee bill number like HB1882 or SB1649."
        )

    html, official_url = await fetch_bill_page(
        bill_number,
        general_assembly,
    )

    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    act_text, caption = extract_caption(
        soup
    )

    events = extract_history(
        soup,
        bill_number,
    )

    if not events:
        raise ValueError(
            f"I could not find an official history table "
            f"for {bill_number} in the "
            f"{general_assembly}th General Assembly."
        )

    latest = events[0]

    chamber = (
        "House"
        if bill_number.startswith("HB")
        else "Senate"
    )

    async with pool.acquire() as connection:

        bill_id = await connection.fetchval(
            """
            INSERT INTO bills (
                general_assembly,
                bill_number,
                chamber,
                title,
                caption,
                status,
                official_url,
                last_action,
                last_action_date,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, NOW()
            )

            ON CONFLICT (
                general_assembly,
                bill_number
            )

            DO UPDATE SET
                chamber = EXCLUDED.chamber,
                title = EXCLUDED.title,
                caption = EXCLUDED.caption,
                status = EXCLUDED.status,
                official_url = EXCLUDED.official_url,
                last_action = EXCLUDED.last_action,
                last_action_date =
                    EXCLUDED.last_action_date,
                updated_at = NOW()

            RETURNING id;
            """,
            general_assembly,
            bill_number,
            chamber,
            act_text,
            caption,
            latest["action"],
            official_url,
            latest["action"],
            latest["action_date"],
        )

        new_events = 0

        for event in events:

            event_key = make_event_key(
                general_assembly,
                bill_number,
                event["action"],
                event["date_text"],
            )

            result = await connection.execute(
                """
                INSERT INTO bill_events (
                    bill_id,
                    event_key,
                    action_date,
                    action,
                    source_url,
                    raw_data
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6
                )

                ON CONFLICT (
                    event_key
                )
                DO NOTHING;
                """,
                bill_id,
                event_key,
                event["action_date"],
                event["action"],
                official_url,
                {
                    "official_date":
                        event["date_text"]
                },
            )

            if result == "INSERT 0 1":
                new_events += 1

    return {
        "bill_number": bill_number,
        "general_assembly":
            general_assembly,
        "official_url": official_url,
        "latest_action":
            latest["action"],
        "latest_action_date":
            latest["date_text"],
        "history_count":
            len(events),
        "new_events":
            new_events,
        "caption": caption,
    }
