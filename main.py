import asyncio
import logging
import os
import sys

import asyncpg
import discord
from discord import app_commands
from discord.ext import commands


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

log = logging.getLogger("tti")


# ---------------------------------------------------------
# Environment variables
# ---------------------------------------------------------

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not DISCORD_TOKEN:
    log.error("DISCORD_TOKEN is missing.")
    sys.exit(1)

if not DATABASE_URL:
    log.error("DATABASE_URL is missing.")
    sys.exit(1)


# ---------------------------------------------------------
# Database
# ---------------------------------------------------------

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS bills (
    id BIGSERIAL PRIMARY KEY,
    general_assembly INTEGER NOT NULL,
    bill_number VARCHAR(20) NOT NULL,
    chamber VARCHAR(20),
    title TEXT,
    caption TEXT,
    status TEXT,
    official_url TEXT,
    last_action TEXT,
    last_action_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (general_assembly, bill_number)
);

CREATE TABLE IF NOT EXISTS bill_events (
    id BIGSERIAL PRIMARY KEY,
    bill_id BIGINT NOT NULL REFERENCES bills(id) ON DELETE CASCADE,
    event_key TEXT NOT NULL UNIQUE,
    action_date TIMESTAMPTZ,
    action TEXT NOT NULL,
    source_url TEXT,
    raw_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS watched_bills (
    id BIGSERIAL PRIMARY KEY,
    discord_user_id BIGINT NOT NULL,
    bill_id BIGINT NOT NULL REFERENCES bills(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (discord_user_id, bill_id)
);

CREATE INDEX IF NOT EXISTS idx_bills_bill_number
ON bills (bill_number);

CREATE INDEX IF NOT EXISTS idx_bill_events_bill_id
ON bill_events (bill_id);

CREATE INDEX IF NOT EXISTS idx_bill_events_action_date
ON bill_events (action_date);
"""


async def create_database_pool():
    for attempt in range(1, 11):
        try:
            pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=5,
                command_timeout=30,
            )

            async with pool.acquire() as connection:
                await connection.execute(CREATE_TABLES_SQL)

            log.info("PostgreSQL connected and tables verified.")
            return pool

        except Exception as exc:
            log.warning(
                "Database connection attempt %s/10 failed: %s",
                attempt,
                exc,
            )

            if attempt == 10:
                raise

            await asyncio.sleep(5)


# ---------------------------------------------------------
# Discord bot
# ---------------------------------------------------------

class TTIBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()

        super().__init__(
            command_prefix="!",
            intents=intents,
        )

        self.db = None

    async def setup_hook(self):
        log.info("Connecting to PostgreSQL...")
        self.db = await create_database_pool()

        log.info("Synchronizing Discord slash commands...")
        synced = await self.tree.sync()

        log.info("Synced %s slash commands.", len(synced))

    async def close(self):
        if self.db:
            await self.db.close()

        await super().close()


bot = TTIBot()


# ---------------------------------------------------------
# Events
# ---------------------------------------------------------

@bot.event
async def on_ready():
    log.info(
        "TTI Legislative Desk is online as %s (ID: %s)",
        bot.user,
        bot.user.id,
    )


# ---------------------------------------------------------
# /ping
# ---------------------------------------------------------

@bot.tree.command(
    name="ping",
    description="Check whether TTI Legislative Desk is online.",
)
async def ping(interaction: discord.Interaction):
    latency_ms = round(bot.latency * 1000)

    await interaction.response.send_message(
        f"🟢 **TTI Legislative Desk is online.**\n"
        f"Discord latency: `{latency_ms} ms`"
    )


# ---------------------------------------------------------
# /dbtest
# ---------------------------------------------------------

@bot.tree.command(
    name="dbtest",
    description="Check the connection to the TTI legislative database.",
)
async def dbtest(interaction: discord.Interaction):
    try:
        async with bot.db.acquire() as connection:
            database_time = await connection.fetchval(
                "SELECT NOW();"
            )

            bill_count = await connection.fetchval(
                "SELECT COUNT(*) FROM bills;"
            )

        await interaction.response.send_message(
            "🟢 **PostgreSQL connection successful.**\n"
            f"Database time: `{database_time}`\n"
            f"Bills currently stored: `{bill_count}`"
        )

    except Exception as exc:
        log.exception("Database test failed.")

        await interaction.response.send_message(
            f"🔴 **Database connection failed.**\n"
            f"`{type(exc).__name__}`",
            ephemeral=True,
        )


# ---------------------------------------------------------
# /bill
# ---------------------------------------------------------

@bot.tree.command(
    name="bill",
    description="Look up a Tennessee bill in the TTI database.",
)
@app_commands.describe(
    bill_number="Bill number, such as HB1882 or SB1649"
)
async def bill(
    interaction: discord.Interaction,
    bill_number: str,
):
    normalized = (
        bill_number
        .upper()
        .replace(" ", "")
        .replace("-", "")
    )

    async with bot.db.acquire() as connection:
        record = await connection.fetchrow(
            """
            SELECT
                general_assembly,
                bill_number,
                title,
                caption,
                status,
                official_url,
                last_action,
                last_action_date
            FROM bills
            WHERE UPPER(REPLACE(bill_number, ' ', '')) = $1
            ORDER BY general_assembly DESC
            LIMIT 1;
            """,
            normalized,
        )

    if not record:
        await interaction.response.send_message(
            f"🔎 **{normalized}** isn't in the TTI database yet.\n\n"
            "That's expected right now. The Tennessee legislative "
            "collector hasn't been connected yet."
        )
        return

    embed = discord.Embed(
        title=record["bill_number"],
        description=record["caption"] or record["title"] or "No description available.",
    )

    embed.add_field(
        name="General Assembly",
        value=str(record["general_assembly"]),
        inline=True,
    )

    embed.add_field(
        name="Status",
        value=record["status"] or "Unknown",
        inline=True,
    )

    if record["last_action"]:
        embed.add_field(
            name="Latest Official Action",
            value=record["last_action"],
            inline=False,
        )

    if record["last_action_date"]:
        embed.add_field(
            name="Action Date",
            value=record["last_action_date"].strftime("%B %d, %Y"),
            inline=False,
        )

    if record["official_url"]:
        embed.add_field(
            name="Official Tennessee Record",
            value=f"[View source]({record['official_url']})",
            inline=False,
        )

    embed.set_footer(
        text="The Tennessee Independent • Legislative Desk"
    )

    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------
# Run
# ---------------------------------------------------------

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
