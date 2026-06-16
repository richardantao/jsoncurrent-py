from __future__ import annotations

from typing import Any, Generic, TypeVar

from .base import TypedEmitter
from .path import get_path, set_path
from .types import FlushFn, JsonCurrentError, MiddlewareFn, Op, StreamingChunk

T = TypeVar("T")


class Collector(TypedEmitter, Generic[T]):
    """
    Client-side inverse of the Emitter.

    Consumes a stream of :class:`StreamingChunk` patch operations and
    reconstructs the original object incrementally. Transport is intentionally
    out of scope: call :meth:`consume` with each chunk as it arrives from
    whatever transport layer your application uses (SSE, WebSocket, HTTP
    streaming, etc.).

    **Events**

    - ``change(state: Partial[T], path: str, op: str)`` — emitted after each data patch is applied
    - ``complete(state: T)`` — emitted when :meth:`complete` is called
    - ``pathstart(path: str, value: Any)`` — first patch on a new path
    - ``pathcomplete(path: str, value: Any)`` — ``complete`` op received for path
    - ``error(err: JsonCurrentError)`` — unrecoverable error applying a patch

    Example::

        collector: Collector[ReportDocument] = Collector()
        collector.on("change", lambda state, path, op: render(state))
        collector.on("complete", lambda final: save(final))

        for chunk in sse_stream:
            collector.consume(chunk)
        collector.complete()

    Example — middleware mirroring ``summary`` to ``original_summary``::

        def mirror_summary(patch: StreamingChunk, next_fn):
            next_fn(patch)
            if patch.path.endswith(".summary"):
                next_fn(StreamingChunk(
                    path=patch.path.replace(".summary", ".original_summary"),
                    value=patch.value,
                    op=patch.op,
                ))

        collector.use(mirror_summary)
    """

    def __init__(self, flush: FlushFn | None = None) -> None:
        super().__init__()
        self._working: dict[str, Any] = {}
        self._state: dict[str, Any] = {}
        self._middleware: list[MiddlewareFn] = []
        self._is_complete: bool = False
        self._seen_paths: set[str] = set()
        self._flush = flush
        self._chunk_queue: list[StreamingChunk] = []
        self._processing_chunks = False
        self._change_queue: list[tuple[dict[str, Any], str, Op]] = []
        self._draining_changes = False

    # -------------------------------------------------------------------------
    # Middleware
    # -------------------------------------------------------------------------

    def use(self, fn: MiddlewareFn) -> "Collector[T]":
        """Register a middleware function. Returns ``self`` for chaining."""
        self._middleware.append(fn)
        return self

    # -------------------------------------------------------------------------
    # Consuming patches
    # -------------------------------------------------------------------------

    def consume(self, chunk: StreamingChunk) -> None:
        """Feed a single patch into the Collector through the middleware chain."""
        if self._is_complete:
            raise JsonCurrentError(
                "Cannot consume patches after complete() has been called. "
                "Call reset() to reuse this Collector."
            )
        self._chunk_queue.append(chunk)
        self._process_chunk_queue()

    def _process_chunk_queue(self) -> None:
        if self._processing_chunks:
            return
        self._processing_chunks = True

        try:
            while self._chunk_queue:
                next_chunk = self._chunk_queue.pop(0)
                self._run_middleware(next_chunk, self._apply_patch)
        finally:
            self._processing_chunks = False
            if self._flush is not None and self._change_queue:
                self._drain_change_queue()

    def _drain_change_queue(self) -> None:
        if self._draining_changes:
            return
        self._draining_changes = True

        try:
            while self._change_queue:
                state, path, op = self._change_queue.pop(0)
                self.emit("change", state, path, op)

                if self._change_queue and self._flush is not None:
                    try:
                        self._flush()
                    except JsonCurrentError as err:
                        self.emit("error", err)
                    except Exception as exc:
                        self.emit("error", JsonCurrentError(
                            "Flush callback failed", exc))
        finally:
            self._draining_changes = False

    def _drain_change_queue_now(self) -> None:
        queued = self._change_queue
        self._change_queue = []
        for state, path, op in queued:
            self.emit("change", state, path, op)

    def complete(self) -> None:
        """Signal that the stream has ended. Emits ``complete`` with final state."""
        if self._is_complete:
            return
        self._is_complete = True

        if self._change_queue:
            self._drain_change_queue_now()

        self.emit("complete", self._state)

    # -------------------------------------------------------------------------
    # State access
    # -------------------------------------------------------------------------

    @property
    def value(self) -> dict[str, Any]:
        """Current partially-assembled state."""
        return self._state

    @property
    def is_complete(self) -> bool:
        return self._is_complete

    # -------------------------------------------------------------------------
    # Reset
    # -------------------------------------------------------------------------

    def reset(self) -> "Collector[T]":
        """
        Reset state and completion flag.

        Preserves registered middleware and event listeners so the instance can
        be reused for a new stream.
        """
        self._working = {}
        self._state = {}
        self._is_complete = False
        self._seen_paths.clear()
        self._chunk_queue = []
        self._processing_chunks = False
        self._change_queue = []
        self._draining_changes = False
        return self

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    def _apply_patch(self, chunk: StreamingChunk) -> None:
        path, value, op = chunk.path, chunk.value, chunk.op

        # ``complete`` patches are informational — fire pathcomplete and return.
        if op == "complete":
            self.emit("pathcomplete", path, value)
            return

        # Fire pathstart BEFORE mutation so the listener sees the initial type.
        if op in ("add", "insert") and path not in self._seen_paths:
            self._seen_paths.add(path)
            start_value = ([] if isinstance(value, list) else {}
                           ) if isinstance(value, (dict, list)) else value
            self.emit("pathstart", path, start_value)

        try:
            if op == "add":
                set_path(self._working, path, value)
            elif op == "append":
                current = get_path(self._working, path, "")
                # pyright: ignore[reportOperatorIssue]
                set_path(self._working, path, (current or "") + value)
            elif op == "insert":
                arr = get_path(self._working, path, [])
                set_path(self._working, path, list(arr) + [value])
            else:
                raise JsonCurrentError(f'Unknown op "{op}" at path "{path}"')
        except JsonCurrentError as err:
            self.emit("error", err)
            return
        except Exception as exc:
            err = JsonCurrentError(
                f'Failed to apply patch at path "{path}"', exc)
            self.emit("error", err)
            return

        # Shallow clone so listeners always receive a new reference
        self._state = {**self._working}

        if self._flush is None:
            self.emit("change", self._state, path, op)
            return

        self._change_queue.append((self._state, path, op))

    def _run_middleware(
        self,
        chunk: StreamingChunk,
        apply: "Collector._ApplyFn",
    ) -> None:
        if not self._middleware:
            apply(chunk)
            return

        middleware = self._middleware

        def run(index: int, current: StreamingChunk) -> None:
            if index >= len(middleware):
                apply(current)
                return
            fn = middleware[index]
            fn(current, lambda next_chunk: run(index + 1, next_chunk))

        run(0, chunk)

    # Type alias for the internal apply callable
    _ApplyFn = Any  # Callable[[StreamingChunk], None]
