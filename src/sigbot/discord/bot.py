"""Discord control bot: slash commands + a liveness watchdog that alerts when the trading bot dies.

Entry point: ``sigbot-discord`` (declared in pyproject.toml). Talks to the bot's token-protected
internal control API (CONTROL_URL); it can halt/resume/pause but can NEVER place orders.
Commands are restricted to DISCORD_ADMIN_USER_ID.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)

MISSES_BEFORE_ALERT = 3
CHECK_EVERY_S = 30


class ControlClient:
    """Minimal client for the trading bot's internal control API.

    The aiohttp session is created lazily on first use, inside a running event loop
    (creating it in __init__ crashes when constructed before asyncio.run).
    """

    def __init__(self, base_url: str, token: str, timeout: float = 5.0) -> None:
        self._base = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._session: aiohttp.ClientSession | None = None
        self.last_error = ""

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._timeout))
        return self._session

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _headers(self) -> dict[str, str]:
        return {"X-Control-Token": self._token}

    async def status(self) -> dict | None:
        try:
            async with self._ensure_session().get(f"{self._base}/status", headers=self._headers()) as resp:
                if resp.status == 200:
                    self.last_error = ""
                    return await resp.json(content_type=None)
                self.last_error = "wrong CONTROL_TOKEN" if resp.status == 401 else f"HTTP {resp.status}"
                log.warning("control /status -> %s", resp.status)
                return None
        except (aiohttp.ClientError, TimeoutError) as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"[:200]
            log.warning("control /status unreachable: %r", exc)
            return None

    async def post(self, path: str, payload: dict | None = None) -> tuple[bool, str]:
        try:
            async with self._ensure_session().post(
                f"{self._base}{path}",
                headers=self._headers(),
                json=payload or {},
            ) as resp:
                body = await resp.json(content_type=None)
                if resp.status == 200:
                    return True, json.dumps(body)[:400]
                return False, f"{resp.status}: {json.dumps(body)[:200]}"
        except (aiohttp.ClientError, TimeoutError) as exc:
            return False, f"unreachable: {exc!r}"


def allowed(user_id: int, admin_id: str) -> bool:
    return str(user_id) == admin_id


class DiscordControlBot:
    def __init__(
        self,
        token: str,
        admin_id: str,
        control: ControlClient,
        alert_channel_id: str,
        guild_id: str | None = None,
    ) -> None:
        self._token = token
        self._admin_id = admin_id
        self._control = control
        self._alert_channel = int(alert_channel_id) if alert_channel_id else 0
        self._guild = discord.Object(id=int(guild_id)) if guild_id else None
        self._liveness_task: asyncio.Task[None] | None = None
        intents = discord.Intents.default()
        self.bot = commands.Bot(command_prefix="!", intents=intents)
        self._register_commands()

    def _register_commands(self) -> None:
        bot = self.bot

        def guard(interaction: discord.Interaction) -> bool:
            user = interaction.user
            return user is not None and allowed(user.id, self._admin_id)

        @bot.tree.command(name="status", description="Trading bot status")
        async def status_cmd(interaction: discord.Interaction) -> None:
            if not guard(interaction):
                await interaction.response.send_message("Not allowed.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            state = await self._control.status()
            if state is None:
                await interaction.followup.send(
                    f"Trading bot unreachable ({self._control.last_error}).",
                    ephemeral=True,
                )
                return
            text = json.dumps(state, indent=1)[:1800]
            await interaction.followup.send(f"```json\n{text}\n```", ephemeral=True)

        @bot.tree.command(name="halt", description="Stop the trading bot (no new orders)")
        @app_commands.describe(reason="Why")
        async def halt_cmd(interaction: discord.Interaction, reason: str = "discord halt") -> None:
            if not guard(interaction):
                await interaction.response.send_message("Not allowed.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            ok, detail = await self._control.post("/halt", {"reason": reason})
            await interaction.followup.send(f"halt: {'OK' if ok else detail}", ephemeral=True)

        @bot.tree.command(name="resume", description="Resume trading after a halt")
        async def resume_cmd(interaction: discord.Interaction) -> None:
            if not guard(interaction):
                await interaction.response.send_message("Not allowed.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            ok, detail = await self._control.post("/resume")
            await interaction.followup.send(f"resume: {'OK' if ok else detail}", ephemeral=True)

        @bot.tree.command(name="pause", description="Pause (or unpause) one strategy")
        @app_commands.describe(strategy="snipe / arb / exits", paused="true = pause, false = trade again")
        async def pause_cmd(interaction: discord.Interaction, strategy: str, paused: bool = True) -> None:
            if not guard(interaction):
                await interaction.response.send_message("Not allowed.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            ok, detail = await self._control.post("/pause", {"strategy": strategy, "paused": paused})
            verb = "pause" if paused else "unpause"
            await interaction.followup.send(f"{verb} {strategy}: {'OK' if ok else detail}", ephemeral=True)

        @bot.tree.command(
            name="tier",
            description="Set and hold the capital tier, or let it move automatically",
        )
        @app_commands.describe(
            level="1-4 holds that tier (step-downs on losses still apply); auto = automatic",
        )
        @app_commands.choices(
            level=[app_commands.Choice(name=v, value=v) for v in ("1", "2", "3", "4", "auto")],
        )
        async def tier_cmd(interaction: discord.Interaction, level: app_commands.Choice[str]) -> None:
            if not guard(interaction):
                await interaction.response.send_message("Not allowed.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            ok, detail = await self._control.post("/tier", {"tier": level.value})
            await interaction.followup.send(f"tier {level.value}: {'OK' if ok else detail}", ephemeral=True)

        @bot.tree.command(name="manual", description="Pause/unpause one exchange")
        @app_commands.describe(exchange_id="SIG exchange id", paused="true = pause")
        async def manual_cmd(interaction: discord.Interaction, exchange_id: str, paused: bool) -> None:
            if not guard(interaction):
                await interaction.response.send_message("Not allowed.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            ok, detail = await self._control.post("/manual", {"exchangeId": exchange_id, "paused": paused})
            await interaction.followup.send(f"manual: {'OK' if ok else detail}", ephemeral=True)

    async def _post_alert(self, message: str) -> None:
        if not self._alert_channel:
            log.error("ALERT (no channel configured) %s", message)
            return
        try:
            channel = await self.bot.fetch_channel(self._alert_channel)
            if isinstance(channel, discord.abc.Messageable):
                await channel.send(message[:2000])
        except discord.DiscordException as exc:
            log.error("failed to post alert: %r", exc)

    async def _liveness(self) -> None:
        misses = 0
        alerted = False
        while not self.bot.is_closed():
            await asyncio.sleep(CHECK_EVERY_S)
            state = await self._control.status()
            if state is None:
                misses += 1
                if misses >= MISSES_BEFORE_ALERT and not alerted:
                    alerted = True
                    await self._post_alert(
                        f"<@{self._admin_id}> the trading bot has been unreachable for "
                        f"{misses * CHECK_EVERY_S}s",
                    )
            else:
                if alerted:
                    alerted = False
                    await self._post_alert("trading bot is reachable again")
                misses = 0

    async def run(self) -> None:
        @self.bot.event
        async def on_ready() -> None:
            # Guild sync makes the commands appear instantly; global sync can take up to an hour.
            if self._guild is not None:
                self.bot.tree.copy_global_to(guild=self._guild)
                await self.bot.tree.sync(guild=self._guild)
            else:
                await self.bot.tree.sync()
            log.info("discord bot ready as %s", self.bot.user)
            # on_ready fires again after every reconnect: start the watchdog only once.
            if self._liveness_task is None or self._liveness_task.done():
                self._liveness_task = asyncio.create_task(self._liveness())

        try:
            await self.bot.start(self._token)
        finally:
            await self._control.close()


def main() -> None:
    token = os.environ.get("DISCORD_BOT_TOKEN")
    if not token:
        log.error("DISCORD_BOT_TOKEN is not set")
        raise SystemExit(1)
    admin_id = os.environ.get("DISCORD_ADMIN_USER_ID", "")
    alert_channel = os.environ.get("DISCORD_ALERT_CHANNEL_ID", "")
    control = ControlClient(
        os.environ.get("CONTROL_URL", "http://127.0.0.1:8081"),
        os.environ.get("CONTROL_TOKEN", "shadow"),
    )
    guild_id = os.environ.get("DISCORD_GUILD_ID") or None
    asyncio.run(DiscordControlBot(token, admin_id, control, alert_channel, guild_id).run())


if __name__ == "__main__":
    main()
