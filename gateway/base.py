"""
Base gateway class. Each exchange implements this interface.
"""
from abc import ABC, abstractmethod
from dataclasses import asdict

from core.process_base import ProcessBase
from core.zmq_channels import Publisher, Puller, MARKET_DATA_PORT, ORDER_PORT
from core.events import EventType
from core.logger import get_logger


class BaseGateway(ProcessBase, ABC):
    """
    Gateway process responsibilities:
    - Connect to exchange (WS/TCP)
    - Publish market data to ZMQ PUB
    - Listen for orders on ZMQ PULL and execute them
    - Report fills back via ZMQ PUB
    """

    def __init__(self, gateway_name: str, **kwargs):
        super().__init__(process_name=f"gateway_{gateway_name}", **kwargs)
        self.gateway_name = gateway_name
        self.logger = get_logger(f"gateway_{gateway_name}")
        self._market_pub: Publisher | None = None
        self._order_puller: Puller | None = None

    async def run(self) -> None:
        self._market_pub = Publisher(MARKET_DATA_PORT)
        self._order_puller = Puller(ORDER_PORT, bind=False)

        await self.connect_exchange()

        await asyncio.gather(
            self._listen_orders(),
            self._stream_market_data(),
        )

    async def _listen_orders(self) -> None:
        """Pull orders from risk engine and execute."""
        import asyncio
        while self.running:
            try:
                order_data = await self._order_puller.pull()
                if order_data.get("gateway") == self.gateway_name:
                    event_type = order_data.get("event_type")
                    if event_type == EventType.ORDER_NEW:
                        await self.send_order(order_data)
                    elif event_type == EventType.ORDER_CANCEL:
                        await self.cancel_order(order_data.get("order_id", ""))
            except Exception as e:
                self.logger.error("Order listener error: %s", e)
                await asyncio.sleep(1)

    async def publish_tick(self, symbol: str, tick: dict) -> None:
        topic = f"tick.{self.gateway_name}.{symbol}"
        tick["event_type"] = EventType.TICK
        tick["gateway"] = self.gateway_name
        tick["symbol"] = symbol
        await self._market_pub.publish(topic, tick)

    async def publish_orderbook(self, symbol: str, book: dict) -> None:
        topic = f"orderbook.{self.gateway_name}.{symbol}"
        book["event_type"] = EventType.ORDERBOOK
        book["gateway"] = self.gateway_name
        book["symbol"] = symbol
        await self._market_pub.publish(topic, book)

    async def publish_fill(self, fill: dict) -> None:
        topic = f"fill.{self.gateway_name}.{fill.get('symbol', '')}"
        fill["event_type"] = EventType.FILL
        fill["gateway"] = self.gateway_name
        await self._market_pub.publish(topic, fill)

    @abstractmethod
    async def connect_exchange(self) -> None:
        """Establish connection to exchange."""
        ...

    @abstractmethod
    async def _stream_market_data(self) -> None:
        """Subscribe and stream market data. Calls publish_tick/publish_orderbook."""
        ...

    @abstractmethod
    async def send_order(self, order: dict) -> str:
        """Send order to exchange. Returns exchange order ID."""
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        """Cancel order on exchange."""
        ...

    @abstractmethod
    async def query_positions(self) -> list[dict]:
        """Query current positions from exchange."""
        ...

    async def on_stop(self) -> None:
        if self._market_pub:
            self._market_pub.close()
        if self._order_puller:
            self._order_puller.close()
        await self.disconnect_exchange()

    async def disconnect_exchange(self) -> None:
        """Override if exchange needs explicit disconnect."""
        pass


# Fix missing import in run()
import asyncio
