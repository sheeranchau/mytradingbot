"""Entry point for web dashboard process."""
import os
import sys
import uvicorn
from core.config import load_env, load_settings

load_env()


def main():
    config = load_settings()
    redis_url = config.get("redis", {}).get("url", "redis://127.0.0.1:6379")
    os.environ.setdefault("REDIS_URL", redis_url)

    loop = "asyncio" if sys.platform == "win32" else "auto"
    uvicorn.run(
        "view.web.app:app",
        host="0.0.0.0",
        port=8080,
        log_level="info",
        loop=loop,
    )


if __name__ == "__main__":
    main()
