"""
Tests for ZeroMQ channel wrappers.
Tests actual message passing over localhost sockets.
"""
import asyncio
import pytest
from tests.conftest import requires_zmq


# Use high ports to avoid conflicts with running system
TEST_PUB_PORT = 18881
TEST_PUSH_PORT = 18882


@requires_zmq
class TestPubSub:

    @pytest.mark.asyncio
    async def test_publish_and_subscribe(self):
        from core.zmq_channels import Publisher, Subscriber
        import zmq.asyncio

        ctx = zmq.asyncio.Context()
        pub = Publisher(TEST_PUB_PORT, ctx=ctx)
        sub = Subscriber(TEST_PUB_PORT, topics=["test."], ctx=ctx)

        # PUB/SUB needs a brief settle time for connection
        await asyncio.sleep(0.1)

        await pub.publish("test.topic", {"key": "value", "num": 42})
        topic, data = await asyncio.wait_for(sub.receive(), timeout=2.0)

        assert topic == "test.topic"
        assert data["key"] == "value"
        assert data["num"] == 42

        sub.close()
        pub.close()
        ctx.term()

    @pytest.mark.asyncio
    async def test_topic_filtering(self):
        from core.zmq_channels import Publisher, Subscriber
        import zmq.asyncio

        ctx = zmq.asyncio.Context()
        pub = Publisher(TEST_PUB_PORT + 1, ctx=ctx)
        # Only subscribe to "tick." topics
        sub = Subscriber(TEST_PUB_PORT + 1, topics=["tick."], ctx=ctx)

        await asyncio.sleep(0.1)

        # Publish on different topics
        await pub.publish("fill.okx.BTC", {"type": "fill"})
        await pub.publish("tick.okx.BTC", {"type": "tick"})

        topic, data = await asyncio.wait_for(sub.receive(), timeout=2.0)
        assert topic == "tick.okx.BTC"
        assert data["type"] == "tick"

        sub.close()
        pub.close()
        ctx.term()

    @pytest.mark.asyncio
    async def test_multiple_subscribers(self):
        from core.zmq_channels import Publisher, Subscriber
        import zmq.asyncio

        ctx = zmq.asyncio.Context()
        pub = Publisher(TEST_PUB_PORT + 2, ctx=ctx)
        sub1 = Subscriber(TEST_PUB_PORT + 2, topics=[""], ctx=ctx)
        sub2 = Subscriber(TEST_PUB_PORT + 2, topics=[""], ctx=ctx)

        await asyncio.sleep(0.1)

        await pub.publish("data", {"msg": "hello"})

        _, data1 = await asyncio.wait_for(sub1.receive(), timeout=2.0)
        _, data2 = await asyncio.wait_for(sub2.receive(), timeout=2.0)

        assert data1["msg"] == "hello"
        assert data2["msg"] == "hello"

        sub1.close()
        sub2.close()
        pub.close()
        ctx.term()


@requires_zmq
class TestPushPull:

    @pytest.mark.asyncio
    async def test_push_and_pull(self):
        from core.zmq_channels import Pusher, Puller
        import zmq.asyncio

        ctx = zmq.asyncio.Context()
        puller = Puller(TEST_PUSH_PORT, bind=True, ctx=ctx)
        pusher = Pusher(TEST_PUSH_PORT, bind=False, ctx=ctx)

        await asyncio.sleep(0.1)

        await pusher.push({"order": "buy", "qty": 0.01})
        data = await asyncio.wait_for(puller.pull(), timeout=2.0)

        assert data["order"] == "buy"
        assert data["qty"] == 0.01

        pusher.close()
        puller.close()
        ctx.term()

    @pytest.mark.asyncio
    async def test_push_pull_ordering(self):
        from core.zmq_channels import Pusher, Puller
        import zmq.asyncio

        ctx = zmq.asyncio.Context()
        puller = Puller(TEST_PUSH_PORT + 1, bind=True, ctx=ctx)
        pusher = Pusher(TEST_PUSH_PORT + 1, bind=False, ctx=ctx)

        await asyncio.sleep(0.1)

        for i in range(5):
            await pusher.push({"seq": i})

        received = []
        for _ in range(5):
            data = await asyncio.wait_for(puller.pull(), timeout=2.0)
            received.append(data["seq"])

        assert received == [0, 1, 2, 3, 4]

        pusher.close()
        puller.close()
        ctx.term()
