"""Fixed-window rate limiting for the ingestion endpoint.

Pure ASGI middleware (no BaseHTTPMiddleware response-buffering pitfalls).
Counts POST /events per client IP in Redis with a window-aligned key:
INCR + EXPIRE is one round trip and works unchanged across multiple API
replicas because the state lives in Redis, not in the process.

Fails open on Redis errors: availability of ingestion is worth more than
strict limiting during a cache outage. Tradeoff documented in ARCHITECTURE.md
(a sliding-window or token-bucket Lua script would smooth the boundary burst
that fixed windows allow).
"""

import json
import logging
import time

from app.config import Settings

logger = logging.getLogger(__name__)


class RateLimitMiddleware:
    def __init__(self, app, settings: Settings) -> None:
        self.app = app
        self.settings = settings

    async def __call__(self, scope, receive, send) -> None:
        if not self._applies(scope):
            await self.app(scope, receive, send)
            return

        client = scope.get("client")
        client_ip = client[0] if client else "unknown"
        window = self.settings.rate_limit_window_seconds
        window_id = int(time.time() // window)
        key = f"ratelimit:{client_ip}:{window_id}"

        try:
            redis = scope["app"].state.redis
            count = await redis.incr(key)
            if count == 1:
                await redis.expire(key, window)
            if count > self.settings.rate_limit_requests:
                logger.warning("rate limited ip=%s count=%d", client_ip, count)
                await _send_429(send, retry_after=window - int(time.time() % window))
                return
        except Exception as exc:
            logger.warning("rate limiter failing open error=%s", exc)

        await self.app(scope, receive, send)

    def _applies(self, scope) -> bool:
        return (
            self.settings.rate_limit_enabled
            and scope["type"] == "http"
            and scope.get("method") == "POST"
            and scope.get("path", "") == "/events"
        )


async def _send_429(send, retry_after: int) -> None:
    body = json.dumps(
        {"error": {"code": "rate_limited", "message": "Too many requests, slow down."}}
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 429,
            "headers": [
                (b"content-type", b"application/json"),
                (b"retry-after", str(max(retry_after, 1)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
