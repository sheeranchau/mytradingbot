"""Entry point for Telegram view process."""
import asyncio
import os

from core.config import load_settings
from view.telegram import TelegramReporter


def main():
    config = load_settings()

    tg_cfg = config["view"]["telegram"]
    reporter = TelegramReporter(
        bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", tg_cfg.get("bot_token", "")),
        chat_id=os.environ.get("TELEGRAM_CHAT_ID", tg_cfg.get("chat_id", "")),
        summary_interval=tg_cfg.get("summary_interval", 300),
    )
    asyncio.run(reporter.start())


if __name__ == "__main__":
    main()
