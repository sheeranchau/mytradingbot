"""
Base class for all trading system processes.
Handles heartbeat, graceful shutdown, and Redis connection.
"""
import asyncio
import os
import signal
import time
from abc import ABC, abstractmethod

from core.redis_store import RedisStore
from core.logger import get_logger


class ProcessBase(ABC):
    """Base class that all runnable processes inherit from."""

    def __init__(self, process_name: str, redis_url: str = "redis://127.0.0.1:6379"):
        self.process_name = process_name
        self.pid = os.getpid()
        self.start_time = time.time()
        self.running = False
        self.redis = RedisStore(url=redis_url)
        self.logger = get_logger(process_name)
        self._heartbeat_interval = 5.0  # seconds

    async def start(self) -> None:
        """Start the process: connect resources, run main loop."""
        self.running = True
        await self.redis.connect()
        self.logger.info("Process started (pid=%d)", self.pid)

        # Register signal handlers (add_signal_handler is Unix-only)
        loop = asyncio.get_event_loop()
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
        except NotImplementedError:
            pass  # Windows — KeyboardInterrupt will still trigger shutdown

        # Run heartbeat and main logic concurrently
        await asyncio.gather(
            self._heartbeat_loop(),
            self.run(),
        )

    async def stop(self) -> None:
        """Graceful shutdown."""
        self.logger.info("Process stopping...")
        self.running = False
        await self.on_stop()
        await self.redis.close()

    async def _heartbeat_loop(self) -> None:
        """Publish heartbeat to Redis."""
        while self.running:
            info = {
                "process_name": self.process_name,
                "pid": str(self.pid),
                "uptime": str(round(time.time() - self.start_time, 1)),
                "timestamp": str(time.time()),
            }
            await self.redis.set_heartbeat(self.process_name, info)
            await asyncio.sleep(self._heartbeat_interval)

    @abstractmethod
    async def run(self) -> None:
        """Main process loop. Subclasses implement this."""
        ...

    async def on_stop(self) -> None:
        """Override for cleanup logic."""
        pass
