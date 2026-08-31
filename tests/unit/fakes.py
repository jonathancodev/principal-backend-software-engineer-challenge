"""Shared in-memory fakes for unit tests."""

import time
from typing import Any, Dict, List, Optional

from redis.exceptions import RedisError

from app.domain.models import EventRecord
from app.errors import DuplicateEventError, StorageUnavailableError


class FakeRepository:
    """In-memory stand-in for MongoEventRepository."""

    def __init__(self, fail_times: int = 0) -> None:
        self.events: List[EventRecord] = []
        self.fail_times = fail_times
        self.insert_calls = 0

    async def insert_event(self, record: EventRecord) -> None:
        self.insert_calls += 1
        if self.insert_calls <= self.fail_times:
            raise StorageUnavailableError("simulated mongo outage")
        if any(e.event_id == record.event_id for e in self.events):
            raise DuplicateEventError(record.event_id)
        self.events.append(record)


class FakeSearchStore:
    def __init__(self, fail: bool = False) -> None:
        self.indexed: List[EventRecord] = []
        self.fail = fail

    async def index_event(self, record: EventRecord) -> None:
        if self.fail:
            raise ConnectionError("simulated es outage")
        self.indexed.append(record)


class FakeDeduplicator:
    """Stateful fake matching RedisDeduplicator's real semantics.

    The original fake returned True on every claim regardless of history,
    which hid a real ordering bug in the worker (a pre-write claim made
    retries look like duplicates). This one keeps actual state so tests
    exercise the same seen/mark contract the Redis implementation has.
    """

    def __init__(self, seen_ids: Optional[set] = None) -> None:
        self.seen_ids = set(seen_ids or ())
        self.checks: List[str] = []
        self.marks: List[str] = []

    async def seen(self, event_id: str) -> bool:
        self.checks.append(event_id)
        return event_id in self.seen_ids

    async def mark(self, event_id: str) -> None:
        self.marks.append(event_id)
        self.seen_ids.add(event_id)


class FakeRedis:
    """Minimal async Redis fake: get/set/delete/incr/expire with TTL support."""

    def __init__(self, fail: bool = False) -> None:
        self.store: Dict[str, Any] = {}
        self.expiries: Dict[str, float] = {}
        self.fail = fail

    def _check(self, key: str) -> None:
        if key in self.expiries and self.expiries[key] <= time.monotonic():
            self.store.pop(key, None)
            self.expiries.pop(key, None)

    async def get(self, key: str):
        if self.fail:
            raise RedisError("simulated redis outage")
        self._check(key)
        return self.store.get(key)

    async def set(self, key: str, value, nx: bool = False, ex: int = None, px: int = None):
        if self.fail:
            raise RedisError("simulated redis outage")
        self._check(key)
        if nx and key in self.store:
            return None
        self.store[key] = value
        if ex is not None:
            self.expiries[key] = time.monotonic() + ex
        if px is not None:
            self.expiries[key] = time.monotonic() + px / 1000.0
        return True

    async def delete(self, key: str):
        if self.fail:
            raise RedisError("simulated redis outage")
        self.store.pop(key, None)
        self.expiries.pop(key, None)

    async def incr(self, key: str) -> int:
        if self.fail:
            raise RedisError("simulated redis outage")
        self._check(key)
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    async def expire(self, key: str, seconds: int):
        if self.fail:
            raise RedisError("simulated redis outage")
        self.expiries[key] = time.monotonic() + seconds

    async def ping(self):
        if self.fail:
            raise RedisError("simulated redis outage")
        return True
