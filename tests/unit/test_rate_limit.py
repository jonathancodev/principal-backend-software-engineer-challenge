"""Unit tests for the rate limiting middleware (fixed window, fail-open)."""

import json

import pytest

from app.config import Settings
from app.middleware.rate_limit import RateLimitMiddleware

from tests.unit.fakes import FakeRedis


class _App:
    """Downstream ASGI app that records whether it was reached."""

    def __init__(self):
        self.called = 0

    async def __call__(self, scope, receive, send):
        self.called += 1
        await send({"type": "http.response.start", "status": 202, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})


class _FastAPIStub:
    def __init__(self, redis):
        class _State:
            pass

        self.state = _State()
        self.state.redis = redis


def _scope(redis, method="POST", path="/events", ip="10.0.0.1"):
    return {
        "type": "http",
        "method": method,
        "path": path,
        "client": (ip, 40000),
        "app": _FastAPIStub(redis),
    }


async def _invoke(middleware, scope):
    responses = []

    async def send(message):
        responses.append(message)

    async def receive():
        return {"type": "http.request"}

    await middleware(scope, receive, send)
    return responses


@pytest.fixture
def redis():
    return FakeRedis()


async def test_requests_under_limit_pass_through(redis):
    app = _App()
    mw = RateLimitMiddleware(app, Settings(rate_limit_requests=5))
    for _ in range(5):
        await _invoke(mw, _scope(redis))
    assert app.called == 5


async def test_requests_over_limit_get_429(redis):
    app = _App()
    mw = RateLimitMiddleware(app, Settings(rate_limit_requests=3))
    responses = []
    for _ in range(4):
        responses = await _invoke(mw, _scope(redis))

    assert app.called == 3
    start = responses[0]
    assert start["status"] == 429
    body = json.loads(responses[1]["body"])
    assert body["error"]["code"] == "rate_limited"


async def test_limit_is_per_client_ip(redis):
    app = _App()
    mw = RateLimitMiddleware(app, Settings(rate_limit_requests=1))
    await _invoke(mw, _scope(redis, ip="10.0.0.1"))
    await _invoke(mw, _scope(redis, ip="10.0.0.2"))
    assert app.called == 2  # separate windows per IP


async def test_non_ingestion_routes_are_not_limited(redis):
    app = _App()
    mw = RateLimitMiddleware(app, Settings(rate_limit_requests=1))
    for _ in range(3):
        await _invoke(mw, _scope(redis, method="GET", path="/events"))
    assert app.called == 3


async def test_redis_outage_fails_open():
    app = _App()
    mw = RateLimitMiddleware(app, Settings(rate_limit_requests=1))
    broken = FakeRedis(fail=True)
    for _ in range(3):
        await _invoke(mw, _scope(broken))
    assert app.called == 3  # availability wins over strict limiting
