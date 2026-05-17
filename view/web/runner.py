"""Entry point for web dashboard process."""
import uvicorn
from core.config import load_env

load_env()


def main():
    uvicorn.run(
        "view.web.app:app",
        host="0.0.0.0",
        port=8080,
        log_level="info",
    )


if __name__ == "__main__":
    main()
