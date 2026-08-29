"""Unit tests for the SQS-style in-process queue."""

import asyncio

import pytest

from app.errors import QueueFullError
from app.queueing.queue import InProcessQueue


@pytest.fixture
async def queue():
    q = InProcessQueue(max_size=10, visibility_timeout_seconds=0.1)
    yield q
    await q.close()


async def test_send_receive_ack_lifecycle(queue):
    await queue.send({"n": 1})
    message = await queue.receive()
    assert message.body == {"n": 1}
    assert message.attempts == 1
    assert queue.in_flight_count() == 1

    await queue.ack(message.receipt_handle)
    assert queue.in_flight_count() == 0
    assert queue.depth() == 0


async def test_backpressure_raises_queue_full():
    q = InProcessQueue(max_size=2, visibility_timeout_seconds=1)
    await q.send({"n": 1})
    await q.send({"n": 2})
    with pytest.raises(QueueFullError):
        await q.send({"n": 3})
    await q.close()


async def test_nack_redelivers_and_increments_attempts(queue):
    await queue.send({"n": 1})
    first = await queue.receive()
    await queue.nack(first.receipt_handle, delay_seconds=0.01)

    redelivered = await asyncio.wait_for(queue.receive(), timeout=1)
    assert redelivered.id == first.id
    assert redelivered.attempts == 2


async def test_visibility_timeout_redelivers_unacked_message(queue):
    """Simulates a worker crash mid-message: never acked, so it comes back."""
    await queue.send({"n": 1})
    first = await queue.receive()
    # Do not ack/nack; visibility timeout (0.1s) should redeliver.
    redelivered = await asyncio.wait_for(queue.receive(), timeout=1)
    assert redelivered.id == first.id
    assert redelivered.attempts == 2


async def test_ack_prevents_visibility_redelivery(queue):
    await queue.send({"n": 1})
    message = await queue.receive()
    await queue.ack(message.receipt_handle)
    await asyncio.sleep(0.2)  # beyond the visibility timeout
    assert queue.depth() == 0


async def test_move_to_dlq(queue):
    await queue.send({"n": 1})
    message = await queue.receive()
    await queue.move_to_dlq(message.receipt_handle, reason="storage down")

    assert queue.dlq.size() == 1
    entry = queue.dlq.peek()[0]
    assert entry["message_id"] == message.id
    assert entry["reason"] == "storage down"
    assert queue.in_flight_count() == 0
