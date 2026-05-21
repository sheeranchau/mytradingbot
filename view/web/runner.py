"""Entry point for web dashboard process."""
import asyncio
import sys
import uvicorn
from core.config import load_env

load_env()


def main():
    # ZMQ requires a selector event loop on Windows (Proactor is the default in 3.8+)
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
