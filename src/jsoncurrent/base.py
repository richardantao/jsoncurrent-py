from __future__ import annotations

from typing import Any, Callable


class TypedEmitter:
    """
    Minimal synchronous event emitter.

    Supports ``on`` / ``off`` / ``once`` / ``emit`` / ``remove_all_listeners``.
    Thread safety is intentionally out of scope — streaming is single-threaded.
    """

    def __init__(self) -> None:
        self._listeners: dict[str, list[Callable[..., Any]]] = {}

    def on(self, event: str, listener: Callable[..., Any]) -> "TypedEmitter":
        """Register a persistent listener for *event*."""
        self._listeners.setdefault(event, []).append(listener)
        return self

    def off(self, event: str, listener: Callable[..., Any]) -> "TypedEmitter":
        """Remove a previously-registered listener."""
        listeners = self._listeners.get(event, [])
        try:
            listeners.remove(listener)
        except ValueError:
            pass
        return self

    def once(self, event: str, listener: Callable[..., Any]) -> "TypedEmitter":
        """Register a one-shot listener that removes itself after the first call."""

        def _wrapper(*args: Any) -> None:
            self.off(event, _wrapper)
            listener(*args)

        self._listeners.setdefault(event, []).append(_wrapper)
        return self

    def emit(self, event: str, *args: Any) -> bool:
        """Invoke all listeners for *event*. Returns True if any listeners fired."""
        listeners = list(self._listeners.get(event, []))
        for listener in listeners:
            listener(*args)
        return bool(listeners)

    def remove_all_listeners(self, event: str | None = None) -> "TypedEmitter":
        """Remove all listeners, optionally scoped to a single *event*."""
        if event is None:
            self._listeners.clear()
        else:
            self._listeners.pop(event, None)
        return self
