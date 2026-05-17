"""
Telegram reporter for the trading system.
Sends alerts, PnL summaries, and supports commands.
"""
import asyncio
import time
from typing import Optional

import aiohttp

from core.process_base import ProcessBase
from core.zmq_channels import Subscriber, MARKET_DATA_PORT, RISK_KILL_PORT
from core.logger import get_logger


class TelegramReporter(ProcessBase):
    """
    View process that reports to Telegram.
    - Subscribes to fills, risk alerts
    - Sends periodic PnL summary
    - Handles commands via Telegram bot polling (/status, /stop, /positions)
    """

    def __init__(self, bot_token: str, chat_id: str,
                 summary_interval: float = 300.0, **kwargs):
        super().__init__(process_name="view_telegram", **kwargs)
        self.logger = get_logger("view_telegram")
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.summary_interval = summary_interval
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_update_id = 0

    async def run(self) -> None:
        self._session = aiohttp.ClientSession()

        # Subscribe to relevant events
        fill_sub = Subscriber(MARKET_DATA_PORT, topics=["fill."])
        kill_sub = Subscriber(RISK_KILL_PORT, topics=["risk."])

        await asyncio.gather(
            self._listen_fills(fill_sub),
            self._listen_kills(kill_sub),
            self._periodic_summary(),
            self._poll_commands(),
        )

    async def _listen_fills(self, sub: Subscriber) -> None:
        while self.running:
            try:
                topic, data = await sub.receive()
                msg = (
                    f"Fill: {data.get('side', '').upper()} "
                    f"{data.get('quantity', 0)} {data.get('symbol', '')} "
                    f"@ {data.get('price', 0):.4f} "
                    f"[{data.get('strategy', '')}]"
                )
                await self._send_message(msg)
            except Exception as e:
                self.logger.error("Fill listener error: %s", e)
                await asyncio.sleep(1)

    async def _listen_kills(self, sub: Subscriber) -> None:
        while self.running:
            try:
                topic, data = await sub.receive()
                strategy = data.get("strategy", "ALL")
                reason = data.get("reason", "unknown")
                msg = f"RISK KILL: Strategy={strategy}, Reason={reason}"
                await self._send_message(msg)
            except Exception as e:
                self.logger.error("Kill listener error: %s", e)
                await asyncio.sleep(1)

    async def _periodic_summary(self) -> None:
        """Send PnL summary periodically."""
        while self.running:
            await asyncio.sleep(self.summary_interval)
            try:
                positions = await self.redis.get_all_positions()
                heartbeats = await self.redis.get_all_heartbeats()

                lines = ["--- Status Summary ---"]
                lines.append(f"Processes: {len(heartbeats)} alive")
                for name, hb in heartbeats.items():
                    uptime = float(hb.get("uptime", 0))
                    lines.append(f"  {name}: up {uptime:.0f}s")

                if positions:
                    lines.append(f"\nPositions ({len(positions)}):")
                    for pos in positions:
                        lines.append(
                            f"  {pos.get('_key', '')}: "
                            f"qty={pos.get('quantity', 0)} "
                            f"pnl={pos.get('unrealized_pnl', 0)}"
                        )
                else:
                    lines.append("\nNo open positions.")

                await self._send_message("\n".join(lines))
            except Exception as e:
                self.logger.error("Summary error: %s", e)

    async def _poll_commands(self) -> None:
        """Poll Telegram for bot commands."""
        while self.running:
            try:
                url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
                params = {"offset": self._last_update_id + 1, "timeout": 10}
                async with self._session.get(url, params=params) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        for update in data.get("result", []):
                            self._last_update_id = update["update_id"]
                            await self._handle_command(update)
            except Exception as e:
                self.logger.error("Poll error: %s", e)
                await asyncio.sleep(5)

    async def _handle_command(self, update: dict) -> None:
        message = update.get("message", {})
        text = message.get("text", "").strip()

        if text == "/status":
            heartbeats = await self.redis.get_all_heartbeats()
            lines = ["Process Status:"]
            for name, hb in heartbeats.items():
                lines.append(f"  {name}: pid={hb.get('pid')} uptime={hb.get('uptime')}s")
            await self._send_message("\n".join(lines) if heartbeats else "No processes reporting.")

        elif text == "/positions":
            positions = await self.redis.get_all_positions()
            if positions:
                lines = ["Open Positions:"]
                for pos in positions:
                    lines.append(f"  {pos.get('_key')}: qty={pos.get('quantity')}")
                await self._send_message("\n".join(lines))
            else:
                await self._send_message("No open positions.")

        elif text == "/stop":
            # Signal all strategies to stop via Redis
            heartbeats = await self.redis.get_all_heartbeats()
            for name in heartbeats:
                if "strategy" in name:
                    await self.redis.set_strategy_enabled(name, False)
            await self._send_message("All strategies disabled.")

        elif text == "/pnl":
            # Fetch latest PnL snapshots
            # This is simplified — a full implementation would aggregate
            await self._send_message("PnL query not yet implemented.")

    async def _send_message(self, text: str) -> None:
        if not self._session:
            return
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with self._session.post(url, json=payload) as resp:
                if resp.status != 200:
                    self.logger.warning("Send failed: %s", await resp.text())
        except Exception as e:
            self.logger.error("Send error: %s", e)

    async def on_stop(self) -> None:
        if self._session:
            await self._session.close()
