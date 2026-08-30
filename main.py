import asyncio
import logging
import os
import re
from datetime import date

import asyncpg
import discord
from discord import app_commands
from discord.ext import commands, tasks
from openai import AsyncOpenAI

from tn_legislation import (
    BillEvent,
    CURRENT_GA,
    TRACKED_GAS,
    discover_bills_for_ga,
    fetch_bill,
    new_client,
    normalize_bill_number,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("tti")

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AI_MODEL = os.getenv("AI_MODEL", "gpt-5.6-luna")

# A rolling sweep of the official Tennessee site.
# 1,000 bills per cycle means a full 5,000-6,000 bill session is normally
# rechecked roughly every 30-40 minutes without hammering the state site.
BILLS_PER_CYCLE = 60
CURRENT_REFRESH_BATCH = 40
HISTORY_BACKFILL_BATCH = 20

SCHEMA = """
CREATE TABLE IF NOT EXISTS guild_config (
    guild_id BIGINT PRIMARY KEY,
    alert_channel_id BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bills (
    id BIGSERIAL PRIMARY KEY,
    general_assembly INTEGER NOT NULL,
    bill_number TEXT NOT NULL,
    official_url TEXT NOT NULL,
    caption TEXT,
    page_text TEXT,
    latest_action TEXT,
    latest_action_date DATE,
    seeded BOOLEAN NOT NULL DEFAULT FALSE,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_checked_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (general_assembly, bill_number)
);

CREATE TABLE IF NOT EXISTS bill_events (
    id BIGSERIAL PRIMARY KEY,
    bill_id BIGINT NOT NULL REFERENCES bills(id) ON DELETE CASCADE,
    event_key TEXT NOT NULL UNIQUE,
    action TEXT NOT NULL,
    action_date_text TEXT NOT NULL,
    action_date DATE,
    scheduled_for DATE,
    category TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bills_ga_number
ON bills (general_assembly, bill_number);

CREATE INDEX IF NOT EXISTS idx_bills_checked
ON bills (general_assembly, last_checked_at);

CREATE INDEX IF NOT EXISTS idx_events_bill
ON bill_events (bill_id, action_date DESC);

CREATE INDEX IF NOT EXISTS idx_events_schedule
ON bill_events (scheduled_for);

CREATE INDEX IF NOT EXISTS idx_bills_search
ON bills USING GIN (
    to_tsvector(
        'english',
        COALESCE(bill_number, '') || ' ' ||
        COALESCE(caption, '') || ' ' ||
        COALESCE(page_text, '')
    )
);
"""


def setting_key(ga: int) -> str:
    return f"baseline_complete:{ga}"


async def get_setting(pool, key: str) -> str | None:
    return await pool.fetchval(
        "SELECT value FROM app_settings WHERE key=$1",
        key,
    )


async def set_setting(pool, key: str, value: str):
    await pool.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (key)
        DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
        """,
        key,
        value,
    )


async def baseline_complete(pool, ga: int) -> bool:
    return (await get_setting(pool, setting_key(ga))) == "true"


async def send_to_alert_channels(bot, embed: discord.Embed):
    rows = await bot.db.fetch(
        "SELECT guild_id, alert_channel_id FROM guild_config"
    )

    for row in rows:
        channel = bot.get_channel(row["alert_channel_id"])
        if channel is None:
            try:
                channel = await bot.fetch_channel(row["alert_channel_id"])
            except Exception:
                log.exception("Could not fetch configured alert channel.")
                continue

        try:
            await channel.send(embed=embed)
        except Exception:
            log.exception("Could not send legislative alert.")


def category_emoji(category: str) -> str:
    return {
        "FILED": "🆕",
        "SCHEDULED": "🗓️",
        "DEFERRED": "⏸️",
        "AMENDED": "🟠",
        "PASSED": "🟣",
        "TO GOVERNOR": "📨",
        "SIGNED": "🟢",
        "VETOED": "🔴",
        "WITHOUT SIGNATURE": "⚪",
        "ENACTED": "✅",
        "UPDATE": "🔵",
    }.get(category, "🔵")


def alert_embed(
    bill_number: str,
    ga: int,
    official_url: str,
    caption: str | None,
    event: BillEvent,
) -> discord.Embed:
    embed = discord.Embed(
        title=f"{category_emoji(event.category)} {event.category}: {bill_number}",
        description=caption or "Tennessee legislative record updated.",
        url=official_url,
    )
    embed.add_field(
        name="Official action",
        value=event.action[:1024],
        inline=False,
    )
    embed.add_field(
        name="Posted",
        value=event.action_date_text,
        inline=True,
    )
    if event.scheduled_for:
        embed.add_field(
            name="Scheduled for consideration",
            value=event.scheduled_for.strftime("%B %d, %Y"),
            inline=True,
        )
    embed.add_field(
        name="Source",
        value=f"[Tennessee General Assembly]({official_url})",
        inline=False,
    )
    embed.set_footer(text=f"The Tennessee Independent • {ga}th General Assembly")
    return embed


class TTIBot(commands.Bot):
    def __init__(self):
        super().__init__(
            command_prefix="!",
            intents=discord.Intents.default(),
        )
        self.db: asyncpg.Pool | None = None
        self.ai = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
        self.scan_lock = asyncio.Lock()

    async def setup_hook(self):
        self.db = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=8,
            command_timeout=45,
        )
        async with self.db.acquire() as conn:
            await conn.execute(SCHEMA)

        synced = await self.tree.sync()
        log.info("Synced %s Discord slash commands.", len(synced))

        if not legislative_watch.is_running():
            legislative_watch.start()

    async def close(self):
        if legislative_watch.is_running():
            legislative_watch.cancel()
        if self.db:
            await self.db.close()
        await super().close()


bot = TTIBot()


@bot.event
async def on_ready():
    log.info("TTI Legislative Desk online as %s", bot.user)


async def save_discovered_bills(ga: int, discovered: dict[str, str]) -> int:
    inserted = 0

    async with bot.db.acquire() as conn:
        for bill_number, official_url in discovered.items():
            result = await conn.execute(
                """
                INSERT INTO bills (
                    general_assembly,
                    bill_number,
                    official_url
                )
                VALUES ($1, $2, $3)
                ON CONFLICT (general_assembly, bill_number)
                DO UPDATE SET official_url=EXCLUDED.official_url
                """,
                ga,
                bill_number,
                official_url,
            )
            if result == "INSERT 0 1":
                inserted += 1

    return inserted


async def sync_one_bill(
    client,
    bill_row,
    *,
    current_baseline_complete: bool,
):
    bill_id = bill_row["id"]
    ga = bill_row["general_assembly"]
    bill_number = bill_row["bill_number"]
    was_seeded = bill_row["seeded"]

    try:
        page = await fetch_bill(
            client,
            bill_number,
            ga,
            bill_row["official_url"],
        )
    except Exception:
        log.exception("Failed fetching %s", bill_number)
        await bot.db.execute(
            "UPDATE bills SET last_checked_at=NOW() WHERE id=$1",
            bill_id,
        )
        return

    new_events: list[BillEvent] = []

    async with bot.db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE bills
                SET
                    official_url=$2,
                    caption=$3,
                    page_text=$4,
                    latest_action=$5,
                    latest_action_date=$6,
                    seeded=TRUE,
                    last_checked_at=NOW(),
                    updated_at=NOW()
                WHERE id=$1
                """,
                bill_id,
                page.official_url,
                page.caption,
                page.page_text,
                page.latest_action,
                page.latest_action_date,
            )

            # Oldest first so alerts read naturally if several changes appeared at once.
            for event in reversed(page.events):
                result = await conn.execute(
                    """
                    INSERT INTO bill_events (
                        bill_id,
                        event_key,
                        action,
                        action_date_text,
                        action_date,
                        scheduled_for,
                        category
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (event_key) DO NOTHING
                    """,
                    bill_id,
                    event.event_key,
                    event.action,
                    event.action_date_text,
                    event.action_date,
                    event.scheduled_for,
                    event.category,
                )
                if result == "INSERT 0 1":
                    new_events.append(event)

    # During the very first historical backfill we intentionally stay quiet.
    # Once the baseline exists, a newly discovered bill IS news and gets alerts.
    should_notify = current_baseline_complete or was_seeded

    if should_notify:
        for event in new_events:
            await send_to_alert_channels(
                bot,
                alert_embed(
                    bill_number,
                    ga,
                    page.official_url,
                    page.caption,
                    event,
                ),
            )


async def run_scan():

    # Never allow two batches to run at once.
    if bot.scan_lock.locked():
        return {
            "status": "busy"
        }

    async with bot.scan_lock:

        log.info("Starting Tennessee legislative batch.")

        async with new_client() as client:

            current_ga = CURRENT_GA

            # -------------------------------------------------
            # DISCOVER CURRENT GENERAL ASSEMBLY
            # -------------------------------------------------

            current_discovered = await discover_bills_for_ga(
                client,
                current_ga,
            )

            newly_discovered = await save_discovered_bills(
                current_ga,
                current_discovered,
            )

            log.info(
                "Current Tennessee index contains %s bills.",
                len(current_discovered),
            )

            # -------------------------------------------------
            # DISCOVER HISTORICAL GENERAL ASSEMBLIES
            #
            # Only do this once for each historical GA.
            # These sessions are finished, so their bill list
            # isn't going to keep changing.
            # -------------------------------------------------

            for ga in reversed(TRACKED_GAS):

                if ga == current_ga:
                    continue

                existing_count = await bot.db.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM bills
                    WHERE general_assembly=$1
                    """,
                    ga,
                )

                if existing_count > 0:
                    continue

                try:

                    historical_bills = await discover_bills_for_ga(
                        client,
                        ga,
                    )

                    await save_discovered_bills(
                        ga,
                        historical_bills,
                    )

                    log.info(
                        "Discovered %s bills for Tennessee GA %s.",
                        len(historical_bills),
                        ga,
                    )

                except Exception:

                    log.exception(
                        "Historical discovery failed for GA %s.",
                        ga,
                    )

            # -------------------------------------------------
            # FIX ANY BAD OLD BASELINE FLAGS
            # -------------------------------------------------

            baseline_cache = {}

            for ga in TRACKED_GAS:

                total = await bot.db.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM bills
                    WHERE general_assembly=$1
                    """,
                    ga,
                )

                unseeded = await bot.db.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM bills
                    WHERE
                        general_assembly=$1
                        AND seeded=FALSE
                    """,
                    ga,
                )

                is_complete = await baseline_complete(
                    bot.db,
                    ga,
                )

                # A previous broken run may have marked
                # an empty/incomplete GA as finished.
                if (
                    total > 0
                    and unseeded > 0
                    and is_complete
                ):

                    await set_setting(
                        bot.db,
                        setting_key(ga),
                        "false",
                    )

                    is_complete = False

                baseline_cache[ga] = is_complete

            # -------------------------------------------------
            # CURRENT GA GETS FIRST PRIORITY
            # -------------------------------------------------

            current_unseeded = await bot.db.fetchval(
                """
                SELECT COUNT(*)
                FROM bills
                WHERE
                    general_assembly=$1
                    AND seeded=FALSE
                """,
                current_ga,
            )

            rows = []

            if current_unseeded > 0:

                # Finish current Tennessee legislature first.

                rows = await bot.db.fetch(
                    """
                    SELECT
                        id,
                        general_assembly,
                        bill_number,
                        official_url,
                        seeded
                    FROM bills
                    WHERE
                        general_assembly=$1
                        AND seeded=FALSE
                    ORDER BY
                        last_checked_at ASC NULLS FIRST,
                        bill_number ASC
                    LIMIT $2
                    """,
                    current_ga,
                    BILLS_PER_CYCLE,
                )

            else:

                # -------------------------------------------------
                # CURRENT SESSION REFRESH
                # -------------------------------------------------

                current_rows = await bot.db.fetch(
                    """
                    SELECT
                        id,
                        general_assembly,
                        bill_number,
                        official_url,
                        seeded
                    FROM bills
                    WHERE
                        general_assembly=$1
                        AND seeded=TRUE
                    ORDER BY
                        last_checked_at ASC NULLS FIRST,
                        bill_number ASC
                    LIMIT $2
                    """,
                    current_ga,
                    CURRENT_REFRESH_BATCH,
                )

                # -------------------------------------------------
                # HISTORICAL BACKFILL
                # -------------------------------------------------

                history_rows = await bot.db.fetch(
                    """
                    SELECT
                        id,
                        general_assembly,
                        bill_number,
                        official_url,
                        seeded
                    FROM bills
                    WHERE
                        general_assembly <> $1
                        AND seeded=FALSE
                    ORDER BY
                        general_assembly DESC,
                        last_checked_at ASC NULLS FIRST,
                        bill_number ASC
                    LIMIT $2
                    """,
                    current_ga,
                    HISTORY_BACKFILL_BATCH,
                )

                rows = (
                    list(current_rows)
                    + list(history_rows)
                )

            # -------------------------------------------------
            # PROCESS THIS SMALL BATCH
            # -------------------------------------------------

            semaphore = asyncio.Semaphore(2)

            async def worker(row):

                async with semaphore:

                    ga = row["general_assembly"]

                    try:

                        await sync_one_bill(
                            client,
                            row,
                            current_baseline_complete=(
                                baseline_cache.get(
                                    ga,
                                    False,
                                )
                            ),
                        )

                    except Exception:

                        log.exception(
                            "Unexpected error processing %s.",
                            row["bill_number"],
                        )

                    # Be polite to Tennessee's server.
                    await asyncio.sleep(0.25)

            results = await asyncio.gather(
                *[
                    worker(row)
                    for row in rows
                ],
                return_exceptions=True,
            )

            for result in results:

                if isinstance(
                    result,
                    Exception,
                ):

                    log.error(
                        "Batch worker failed: %s",
                        result,
                    )

            # -------------------------------------------------
            # CHECK WHICH GENERAL ASSEMBLIES ARE FINISHED
            # -------------------------------------------------

            for ga in TRACKED_GAS:

                total = await bot.db.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM bills
                    WHERE general_assembly=$1
                    """,
                    ga,
                )

                if total == 0:
                    continue

                remaining = await bot.db.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM bills
                    WHERE
                        general_assembly=$1
                        AND seeded=FALSE
                    """,
                    ga,
                )

                if (
                    remaining == 0
                    and not await baseline_complete(
                        bot.db,
                        ga,
                    )
                ):

                    await set_setting(
                        bot.db,
                        setting_key(ga),
                        "true",
                    )

                    log.info(
                        "Baseline complete for Tennessee GA %s.",
                        ga,
                    )

                    # Only announce completion of the
                    # CURRENT legislature in Discord.
                    if ga == current_ga:

                        embed = discord.Embed(
                            title=(
                                "✅ Current Tennessee "
                                "legislative index complete"
                            ),
                            description=(
                                "The current General Assembly "
                                "has been indexed. Future official "
                                "changes will generate alerts."
                            ),
                        )

                        await send_to_alert_channels(
                            bot,
                            embed,
                        )

            # -------------------------------------------------
            # RETURN CLEAN PROGRESS DATA
            # -------------------------------------------------

            current_total = await bot.db.fetchval(
                """
                SELECT COUNT(*)
                FROM bills
                WHERE general_assembly=$1
                """,
                current_ga,
            )

            current_indexed = await bot.db.fetchval(
                """
                SELECT COUNT(*)
                FROM bills
                WHERE
                    general_assembly=$1
                    AND seeded=TRUE
                """,
                current_ga,
            )

            historical_remaining = await bot.db.fetchval(
                """
                SELECT COUNT(*)
                FROM bills
                WHERE
                    general_assembly <> $1
                    AND seeded=FALSE
                """,
                current_ga,
            )

            result = {
                "status": "ok",
                "current_ga": current_ga,
                "current_total": current_total,
                "current_indexed": current_indexed,
                "historical_remaining": historical_remaining,
                "processed_this_batch": len(rows),
                "new_current_bills": newly_discovered,
            }

            log.info(
                "Tennessee legislative batch complete: %s",
                result,
            )

            return result

@tasks.loop(minutes=5)
async def legislative_watch():
    try:
        result = await run_scan()
        log.info("Legislative scan: %s", result)
    except Exception:
        log.exception("Legislative watcher cycle failed.")


@legislative_watch.before_loop
async def before_legislative_watch():
    await bot.wait_until_ready()


@bot.tree.command(
    name="setalerts",
    description="Send automatic Tennessee legislative alerts to this channel.",
)
@app_commands.checks.has_permissions(manage_guild=True)
async def setalerts(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "Run this inside the TTI Discord server.",
            ephemeral=True,
        )
        return

    await bot.db.execute(
        """
        INSERT INTO guild_config (guild_id, alert_channel_id, updated_at)
        VALUES ($1,$2,NOW())
        ON CONFLICT (guild_id)
        DO UPDATE SET alert_channel_id=EXCLUDED.alert_channel_id, updated_at=NOW()
        """,
        interaction.guild.id,
        interaction.channel_id,
    )

    await interaction.response.send_message(
        "✅ Automatic Tennessee legislative alerts will post in this channel."
    )


@bot.tree.command(
    name="status",
    description="Show the Tennessee legislative monitor status.",
)
async def status(interaction: discord.Interaction):
    await interaction.response.defer()

    general_assembly = await bot.db.fetchval(
        "SELECT MAX(general_assembly) FROM bills"
    )
    if not general_assembly:
        embed = discord.Embed(
            title="🏛️ TTI Legislative Desk",
            description=(
                "The Tennessee bill index has not populated yet. "
                "The watcher will retry automatically, or an administrator can run `/scan`."
            ),
        )
        await interaction.followup.send(embed=embed)
        return

    total = await bot.db.fetchval(
        "SELECT COUNT(*) FROM bills WHERE general_assembly=$1",
        general_assembly,
    )
    indexed = await bot.db.fetchval(
        "SELECT COUNT(*) FROM bills WHERE general_assembly=$1 AND seeded=TRUE",
        general_assembly,
    )
    last_checked = await bot.db.fetchval(
        "SELECT MAX(last_checked_at) FROM bills WHERE general_assembly=$1",
        general_assembly,
    )
    complete = await baseline_complete(bot.db, general_assembly)

    percent = (indexed / total * 100) if total else 0

    embed = discord.Embed(
        title="🏛️ Tennessee Legislative Monitor",
        description=f"**{general_assembly}th General Assembly (2025–2026)**",
    )
    embed.add_field(
        name="Bills found",
        value=f"{total:,}",
        inline=True,
    )
    embed.add_field(
        name="Fully indexed",
        value=f"{indexed:,} ({percent:.1f}%)",
        inline=True,
    )
    embed.add_field(
        name="Initial index",
        value="✅ Complete" if complete else "⏳ Building",
        inline=True,
    )
    embed.add_field(
        name="Last official-site check",
        value=str(last_checked) if last_checked else "Starting now",
        inline=False,
    )
    embed.set_footer(
        text="Source: Tennessee General Assembly official legislative records"
    )
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="bill",
    description="Look up a Tennessee bill from the official legislative record.",
)
@app_commands.describe(bill_number="Example: HB1882")
async def bill(interaction: discord.Interaction, bill_number: str):
    number = normalize_bill_number(bill_number)

    row = await bot.db.fetchrow(
        """
        SELECT *
        FROM bills
        WHERE bill_number=$1
        ORDER BY general_assembly DESC
        LIMIT 1
        """,
        number,
    )

    if not row:
        await interaction.response.send_message(
            f"I don't have **{number}** indexed yet.",
            ephemeral=True,
        )
        return

    events = await bot.db.fetch(
        """
        SELECT action, action_date_text, category
        FROM bill_events
        WHERE bill_id=$1
        ORDER BY action_date DESC NULLS LAST, id DESC
        LIMIT 5
        """,
        row["id"],
    )

    embed = discord.Embed(
        title=f"{number} • {row['general_assembly']}th General Assembly",
        description=row["caption"] or "No caption available.",
        url=row["official_url"],
    )

    if row["latest_action"]:
        embed.add_field(
            name="Latest official action",
            value=row["latest_action"][:1024],
            inline=False,
        )

    if events:
        history = "\n".join(
            f"**{e['action_date_text']}** — {e['action']}"
            for e in events
        )
        embed.add_field(
            name="Recent history",
            value=history[:1024],
            inline=False,
        )

    embed.add_field(
        name="Official source",
        value=f"[Tennessee General Assembly]({row['official_url']})",
        inline=False,
    )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="search",
    description="Search Tennessee legislation by topic or phrase.",
)
@app_commands.describe(query="Example: hemp, property tax, school vouchers")
async def search(interaction: discord.Interaction, query: str):
    await interaction.response.defer()

    rows = await bot.db.fetch(
        """
        WITH q AS (
            SELECT websearch_to_tsquery('english', $1) AS query
        )
        SELECT
            bill_number,
            general_assembly,
            caption,
            latest_action,
            official_url,
            ts_rank_cd(
                to_tsvector(
                    'english',
                    COALESCE(bill_number,'') || ' ' ||
                    COALESCE(caption,'') || ' ' ||
                    COALESCE(page_text,'')
                ),
                q.query
            ) AS rank
        FROM bills, q
        WHERE
            to_tsvector(
                'english',
                COALESCE(bill_number,'') || ' ' ||
                COALESCE(caption,'') || ' ' ||
                COALESCE(page_text,'')
            ) @@ q.query
            OR bill_number ILIKE '%' || $1 || '%'
        ORDER BY rank DESC, latest_action_date DESC NULLS LAST
        LIMIT 10
        """,
        query,
    )

    if not rows:
        await interaction.followup.send(
            f"No indexed Tennessee bills matched **{query}**."
        )
        return

    lines = []
    for row in rows:
        caption = (row["caption"] or "No caption").replace("\n", " ")
        if len(caption) > 220:
            caption = caption[:217] + "..."
        lines.append(
            f"**[{row['bill_number']}]({row['official_url']})** — {caption}"
        )

    await interaction.followup.send("\n\n".join(lines)[:4000])


@bot.tree.command(
    name="upcoming",
    description="Show bills officially scheduled for upcoming consideration.",
)
@app_commands.describe(days="How many days ahead to show, 1-30")
async def upcoming(interaction: discord.Interaction, days: app_commands.Range[int, 1, 30] = 7):
    rows = await bot.db.fetch(
        """
        SELECT DISTINCT ON (b.bill_number, e.scheduled_for)
            b.bill_number,
            b.caption,
            b.official_url,
            e.action,
            e.scheduled_for
        FROM bill_events e
        JOIN bills b ON b.id=e.bill_id
        WHERE
            e.scheduled_for >= CURRENT_DATE
            AND e.scheduled_for <= CURRENT_DATE + $1::int
        ORDER BY
            b.bill_number,
            e.scheduled_for,
            e.id DESC
        LIMIT 30
        """,
        days,
    )

    if not rows:
        await interaction.response.send_message(
            f"No indexed bills are currently shown as scheduled in the next {days} days."
        )
        return

    rows = sorted(rows, key=lambda r: (r["scheduled_for"], r["bill_number"]))
    lines = [
        f"**{r['scheduled_for'].strftime('%b %d')} — "
        f"[{r['bill_number']}]({r['official_url']})**\n{r['action']}"
        for r in rows
    ]

    await interaction.response.send_message(
        "🗓️ **Upcoming Tennessee legislative consideration**\n\n"
        + "\n\n".join(lines)[:3800]
    )


async def retrieve_for_question(question: str):
    number_match = re.search(r"\b(HB|SB)\s*[- ]?(\d{1,4})\b", question, re.I)

    if number_match:
        number = f"{number_match.group(1).upper()}{int(number_match.group(2)):04d}"
        rows = await bot.db.fetch(
            """
            SELECT *
            FROM bills
            WHERE bill_number=$1
            ORDER BY general_assembly DESC
            LIMIT 2
            """,
            number,
        )
    else:
        rows = await bot.db.fetch(
            """
            WITH q AS (
                SELECT websearch_to_tsquery('english', $1) AS query
            )
            SELECT b.*
            FROM bills b, q
            WHERE
                to_tsvector(
                    'english',
                    COALESCE(b.bill_number,'') || ' ' ||
                    COALESCE(b.caption,'') || ' ' ||
                    COALESCE(b.page_text,'')
                ) @@ q.query
            ORDER BY
                ts_rank_cd(
                    to_tsvector(
                        'english',
                        COALESCE(b.bill_number,'') || ' ' ||
                        COALESCE(b.caption,'') || ' ' ||
                        COALESCE(b.page_text,'')
                    ),
                    q.query
                ) DESC,
                b.latest_action_date DESC NULLS LAST
            LIMIT 8
            """,
            question,
        )

    records = []
    for row in rows:
        events = await bot.db.fetch(
            """
            SELECT action, action_date_text, category
            FROM bill_events
            WHERE bill_id=$1
            ORDER BY action_date DESC NULLS LAST, id DESC
            LIMIT 10
            """,
            row["id"],
        )
        records.append((row, events))

    return records


@bot.tree.command(
    name="ask",
    description="Ask a sourced question about Tennessee legislation.",
)
@app_commands.describe(question="Ask about a bill, topic, vote, status, or legislative action")
async def ask(interaction: discord.Interaction, question: str):
    await interaction.response.defer(thinking=True)

    if bot.ai is None:
        await interaction.followup.send(
            "The research assistant is not configured yet. "
            "Add OPENAI_API_KEY to the TTI Railway service."
        )
        return

    records = await retrieve_for_question(question)

    if not records:
        await interaction.followup.send(
            "I couldn't find enough matching official Tennessee legislative records "
            "in the TTI database to answer that reliably."
        )
        return

    context_parts = []
    source_lines = []

    for row, events in records:
        context_parts.append(
            "\n".join(
                [
                    f"BILL: {row['bill_number']}",
                    f"GENERAL ASSEMBLY: {row['general_assembly']}",
                    f"CAPTION: {row['caption'] or 'Not available'}",
                    f"LATEST ACTION: {row['latest_action'] or 'Not available'}",
                    "RECENT OFFICIAL HISTORY:",
                    *[
                        f"- {e['action_date_text']}: {e['action']}"
                        for e in events
                    ],
                    f"OFFICIAL URL: {row['official_url']}",
                ]
            )
        )
        source_lines.append(
            f"[{row['bill_number']}]({row['official_url']})"
        )

    system_prompt = """
You are the research assistant for The Tennessee Independent Legislative Desk.

Answer ONLY from the official Tennessee General Assembly records supplied in
the context. Do not fill gaps from memory. If the records are insufficient,
say exactly what cannot be verified.

Be plainspoken and concise. Distinguish:
- a bill being filed
- being scheduled
- passing one chamber
- passing both chambers
- transmission to the governor
- signature
- veto
- return without signature
- enactment/public chapter

When describing a factual legislative action, identify the bill number.
Do not invent vote totals, dates, sponsors, motives, or legal effects.
"""

    user_prompt = (
        f"QUESTION:\n{question}\n\n"
        f"OFFICIAL TENNESSEE RECORDS:\n\n"
        + "\n\n---\n\n".join(context_parts)
    )

    try:
        response = await bot.ai.responses.create(
            model=AI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        answer = response.output_text.strip()
    except Exception:
        log.exception("AI question answering failed.")
        await interaction.followup.send(
            "The official-record search worked, but the AI explanation failed. "
            "Check the Railway logs."
        )
        return

    if len(answer) > 3300:
        answer = answer[:3297] + "..."

    sources = " • ".join(dict.fromkeys(source_lines))
    await interaction.followup.send(
        f"{answer}\n\n**Official Tennessee sources:** {sources}"[:4000]
    )


@bot.tree.command(
    name="scan",
    description="Check Tennessee's official legislative index now.",
)
@app_commands.checks.has_permissions(manage_guild=True)
async def scan(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True, ephemeral=True)

    try:
        result = await run_scan()
    except Exception as exc:
        log.exception("Manual Tennessee legislative scan failed.")
        await interaction.followup.send(
            "🔴 **The Tennessee scan failed.**\n"
            f"`{type(exc).__name__}: {exc}`",
            ephemeral=True,
        )
        return

    if result.get("status") == "already running":
        await interaction.followup.send(
            "⏳ A Tennessee legislative scan is already running.",
            ephemeral=True,
        )
        return

    general_assembly = result["general_assembly"]
    embed = discord.Embed(
        title="✅ Tennessee legislative scan finished",
        description=f"**{general_assembly}th General Assembly (2025–2026)**",
    )
    embed.add_field(
        name="Bills found on official index",
        value=f"{result['discovered']:,}",
        inline=True,
    )
    embed.add_field(
        name="Indexed so far",
        value=f"{result['indexed']:,}",
        inline=True,
    )
    embed.add_field(
        name="Checked this run",
        value=f"{result['checked_this_run']:,}",
        inline=True,
    )
    embed.add_field(
        name="Still to index",
        value=f"{result['remaining']:,}",
        inline=True,
    )
    embed.add_field(
        name="Initial index",
        value="✅ Complete" if result["baseline_complete"] else "⏳ Building",
        inline=True,
    )
    embed.set_footer(
        text="Official source: Tennessee General Assembly"
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(
    name="ping",
    description="Check whether TTI Legislative Desk is online.",
)
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message(
        f"🟢 TTI Legislative Desk is online. `{round(bot.latency * 1000)} ms`"
    )


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
