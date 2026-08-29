"""Structured-ish logging configuration.

Plain stdlib logging with a consistent key=value style so logs are grep-able
and machine-parseable without pulling in an extra dependency. In production
this would be swapped for JSON logs shipped to an aggregator.
"""

import logging
import sys

_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    if root.handlers:
        # Already configured (e.g. by uvicorn or a previous test app); just set level.
        root.setLevel(level.upper())
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT))
    root.addHandler(handler)
    root.setLevel(level.upper())
    # Quiet noisy dependency loggers.
    for noisy in ("elastic_transport", "pymongo", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
