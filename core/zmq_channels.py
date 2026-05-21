"""
ZeroMQ channel definitions and helpers.
Defines the topology of all ZMQ sockets used in the system.
"""
import zmq
import zmq.asyncio
import msgpack
from typing import Optional


# --- Port assignments ---
# Per-gateway ports — each gateway owns its own market-data PUB and orders PULL.
# Subscribers (strategy, risk, view) use MultiSubscriber to merge feeds.
# Risk engine keeps one Pusher per gateway, picks by order["gateway"].
GATEWAY_PORTS = {
    "okx":    {"market_data": 15555, "orders": 15557},
    "futu":   {"market_data": 15565, "orders": 15567},
    "webull": {"market_data": 15575, "orders": 15577},
}

# Convenience flat lists for MultiSubscriber / multi-Pusher consumers
GATEWAY_MARKET_DATA_PORTS = [p["market_data"] for p in GATEWAY_PORTS.values()]
GATEWAY_ORDER_PORTS = {gw: p["orders"] for gw, p in GATEWAY_PORTS.items()}

# Legacy aliases (default to OKX); new code should use GATEWAY_PORTS
MARKET_DATA_PORT = GATEWAY_PORTS["okx"]["market_data"]
ORDER_PORT = GATEWAY_PORTS["okx"]["orders"]

# Order flow: strategy -> risk -> gateway (PUSH/PULL pipeline)
SIGNAL_PORT = 15556       # strategy pushes signals
# Risk kill: PUB/SUB (risk publishes kill signals, all subscribe)
RISK_KILL_PORT = 15558
# Heartbeat: PUB/SUB (all processes publish, supervisor subscribes)
HEARTBEAT_PORT = 15559
# View fan-out: PUB/SUB (all events mirrored for view consumption)
VIEW_PORT = 15560


def get_address(port: int, host: str = "127.0.0.1") -> str:
    return f"tcp://{host}:{port}"


class Publisher:
    """ZMQ PUB socket wrapper."""

    def __init__(self, port: int, ctx: Optional[zmq.asyncio.Context] = None):
        self.ctx = ctx or zmq.asyncio.Context.instance()
        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.bind(get_address(port))

    async def publish(self, topic: str, data: dict) -> None:
        payload = msgpack.packb(data, use_bin_type=True)
        await self.socket.send_multipart([topic.encode(), payload])

    def close(self):
        self.socket.close()


class Subscriber:
    """ZMQ SUB socket wrapper."""

    def __init__(self, port: int, topics: list[str] = None,
                 ctx: Optional[zmq.asyncio.Context] = None):
        self.ctx = ctx or zmq.asyncio.Context.instance()
        self.socket = self.ctx.socket(zmq.SUB)
        self.socket.connect(get_address(port))
        topics = topics or [""]
        for topic in topics:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    async def receive(self) -> tuple[str, dict]:
        topic_bytes, payload = await self.socket.recv_multipart()
        data = msgpack.unpackb(payload, raw=False)
        return topic_bytes.decode(), data

    def close(self):
        self.socket.close()


class MultiSubscriber:
    """SUB socket connected to multiple endpoints (one per gateway).
    ZMQ allows a single SUB to connect to multiple PUB endpoints; messages
    from all of them are interleaved on the same recv() stream."""

    def __init__(self, ports: list, topics: list = None,
                 ctx: Optional[zmq.asyncio.Context] = None):
        self.ctx = ctx or zmq.asyncio.Context.instance()
        self.socket = self.ctx.socket(zmq.SUB)
        for port in ports:
            self.socket.connect(get_address(port))
        topics = topics or [""]
        for topic in topics:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    async def receive(self) -> tuple[str, dict]:
        topic_bytes, payload = await self.socket.recv_multipart()
        data = msgpack.unpackb(payload, raw=False)
        return topic_bytes.decode(), data

    def close(self):
        self.socket.close()


class Pusher:
    """ZMQ PUSH socket wrapper (for pipeline pattern)."""

    def __init__(self, port: int, bind: bool = False,
                 ctx: Optional[zmq.asyncio.Context] = None):
        self.ctx = ctx or zmq.asyncio.Context.instance()
        self.socket = self.ctx.socket(zmq.PUSH)
        addr = get_address(port)
        if bind:
            self.socket.bind(addr)
        else:
            self.socket.connect(addr)

    async def push(self, data: dict) -> None:
        payload = msgpack.packb(data, use_bin_type=True)
        await self.socket.send(payload)

    def close(self):
        self.socket.close()


class Puller:
    """ZMQ PULL socket wrapper (for pipeline pattern)."""

    def __init__(self, port: int, bind: bool = False,
                 ctx: Optional[zmq.asyncio.Context] = None):
        self.ctx = ctx or zmq.asyncio.Context.instance()
        self.socket = self.ctx.socket(zmq.PULL)
        addr = get_address(port)
        if bind:
            self.socket.bind(addr)
        else:
            self.socket.connect(addr)

    async def pull(self) -> dict:
        payload = await self.socket.recv()
        return msgpack.unpackb(payload, raw=False)

    def close(self):
        self.socket.close()
