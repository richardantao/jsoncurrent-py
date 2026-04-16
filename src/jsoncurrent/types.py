from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal

Op = Literal["add", "append", "insert", "complete"]
"""Patch operation type."""

MiddlewareFn = Callable[["StreamingChunk", Callable[["StreamingChunk"], None]], None]
"""Middleware function: receives the current patch and a ``next`` callable."""


@dataclass
class StreamingChunk:
    """A single patch emitted by the Emitter and consumed by the Collector."""

    path: str
    value: Any
    op: Op


class JsonCurrentError(Exception):
    """Raised when the Emitter or Collector encounters an unrecoverable error."""

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause
