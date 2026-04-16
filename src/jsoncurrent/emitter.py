from __future__ import annotations

from typing import Any

from .base import TypedEmitter
from .types import JsonCurrentError, StreamingChunk

# ---------------------------------------------------------------------------
# FSM state constants
# ---------------------------------------------------------------------------
_START = 0
_OBJECT_KEY = 1       # inside an object, expecting a quoted key
_OBJECT_KEY_STR = 2   # inside the key string itself
_AFTER_KEY = 3        # key parsed, expecting ':'
_OBJECT_VALUE = 4     # after ':', expecting a value
_STRING_VALUE = 5     # inside a string value
_NUMBER_VALUE = 6     # accumulating digits / decimal / exponent
_BOOLEAN_VALUE = 7    # accumulating t/f/r/u/e/a/l/s literal
_NULL_VALUE = 8       # accumulating n/u/l/l literal
_ARRAY_VALUE = 9      # inside an array, expecting a value or ']'
_AFTER_VALUE = 10     # after a complete value, expecting ',' or close
_UNICODE_ESCAPE = 11  # inside \uXXXX sequence

# ---------------------------------------------------------------------------
# Character code constants (avoid repeated ord() calls in the hot path)
# ---------------------------------------------------------------------------
_CC_QUOTE = 0x22      # "
_CC_COMMA = 0x2C      # ,
_CC_MINUS = 0x2D      # -
_CC_SLASH_FWD = 0x2F  # /
_CC_COLON = 0x3A      # :
_CC_LBRACKET = 0x5B   # [
_CC_BACKSLASH = 0x5C  # \
_CC_RBRACKET = 0x5D   # ]
_CC_LBRACE = 0x7B     # {
_CC_RBRACE = 0x7D     # }
_CC_b = 0x62
_CC_f = 0x66
_CC_n = 0x6E
_CC_r = 0x72
_CC_t = 0x74
_CC_u = 0x75
_CC_0 = 0x30
_CC_9 = 0x39
_CC_SP = 0x20   # space
_CC_NL = 0x0A   # \n
_CC_CR = 0x0D   # \r
_CC_TAB = 0x09  # \t
_CC_DOT = 0x2E  # .
_CC_e = 0x65    # e
_CC_E = 0x45    # E
_CC_PLUS = 0x2B  # +


# ---------------------------------------------------------------------------
# Internal stack frame
# ---------------------------------------------------------------------------

class _StackEntry:
    __slots__ = ("type", "base_path", "index", "value")

    def __init__(self, type_: int, base_path: str, index: int, value: Any) -> None:
        self.type = type_       # 0 = object/key frame, 1 = array frame
        self.base_path = base_path
        self.index = index      # -1 = not yet incremented (arrays)
        self.value = value      # accumulated value for this frame


# ---------------------------------------------------------------------------
# Emitter
# ---------------------------------------------------------------------------

class Emitter(TypedEmitter):
    """
    Server-side streaming JSON parser.

    Consumes raw JSON tokens as they arrive from an LLM response stream and
    emits :class:`StreamingChunk` patch operations. Transport is intentionally
    out of scope — feed tokens via :meth:`write` from any source (OpenAI SDK,
    Anthropic SDK, etc.) and call :meth:`flush` when the stream ends.

    Each emitted chunk follows the ``StreamingChunk`` contract:
    ``StreamingChunk(path, value, op)`` where op is ``'add' | 'append' | 'complete'``.

    **Events**

    - ``patch(chunk: StreamingChunk)`` — emitted for each incremental update
    - ``complete()`` — emitted after :meth:`flush`
    - ``error(err: JsonCurrentError)`` — emitted on unrecoverable parse errors

    Example — with OpenAI SDK::

        from jsoncurrent import Emitter

        emitter = Emitter(root="prediction")
        emitter.on("patch", lambda c: sse.send(c))
        emitter.on("complete", lambda: sse.close())

        stream = client.chat.completions.create(stream=True, ...)
        for event in stream:
            delta = event.choices[0].delta.content or ""
            emitter.write(delta)
        emitter.flush()
    """

    def __init__(self, root: str = "", completions: bool = True) -> None:
        super().__init__()
        self._root = root
        self._completions = completions
        # FSM state
        self._state: int = _START
        self._stack: list[_StackEntry] = []
        # Persistent buffer + position (cross-token state)
        self._buf: str = ""
        self._pos: int = 0
        # Accumulators
        self._key_buf: str = ""   # object key being assembled
        self._val_buf: str = ""   # number / boolean / null literal
        # String-value tracking
        self._string_initialised: bool = False
        self._pending_escape: bool = False
        # Unicode escape
        self._unicode_buf: str = ""
        self._high_surrogate: int = 0

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def write(self, token: str) -> None:
        """Feed a raw JSON token. May emit zero or more ``patch`` events."""
        self._buf += token
        self._process()
        # Trim consumed portion to prevent unbounded buffer growth
        if self._pos > 0:
            self._buf = self._buf[self._pos:]
            self._pos = 0

    def flush(self) -> None:
        """Signal end of stream. Finalises pending primitives then emits ``complete``."""
        self._finalise()
        self.emit("complete")
        self.reset()

    def reset(self) -> None:
        """Reset internal state without emitting ``complete``."""
        self._state = _START
        self._stack = []
        self._buf = ""
        self._pos = 0
        self._key_buf = ""
        self._val_buf = ""
        self._string_initialised = False
        self._pending_escape = False
        self._unicode_buf = ""
        self._high_surrogate = 0

    # -------------------------------------------------------------------------
    # Path resolution
    # -------------------------------------------------------------------------

    @property
    def _current_path(self) -> str:
        if not self._stack:
            return self._root
        top = self._stack[-1]
        if top.type == 1:  # array
            return top.base_path if top.index < 0 else f"{top.base_path}[{top.index}]"
        return top.base_path  # object / key frame

    # -------------------------------------------------------------------------
    # Stack manipulation
    # -------------------------------------------------------------------------

    def _push_container(self, name: str, type_: int) -> None:
        parent = self._stack[-1] if self._stack else None

        if parent is None:
            # Root container: use root prefix if set, otherwise fall back to name
            base_path = self._root if self._root else name
        elif parent.type == 1:  # array parent
            element_path = self._current_path
            base_path = f"{element_path}.{name}" if name else element_path
        else:  # object parent
            if name:
                base_path = f"{parent.base_path}.{name}" if parent.base_path else name
            else:
                base_path = parent.base_path

        value: Any = [] if type_ == 1 else {}
        self._stack.append(_StackEntry(type_, base_path, -1, value))

    def _push_key(self, key: str) -> None:
        parent = self._stack[-1] if self._stack else None
        base = parent.base_path if parent is not None else self._root
        path = f"{base}.{key}" if base else key
        self._stack.append(_StackEntry(0, path, -1, None))

    def _pop_stack(self) -> None:
        """Close a container (object or array), emit ``complete``, propagate value."""
        if not self._stack:
            return
        top = self._stack[-1]
        # Arrays complete at their base path; objects complete at current path
        completing_path = top.base_path if top.type == 1 else self._current_path

        if self._completions:
            self.emit(
                "patch",
                StreamingChunk(
                    path=completing_path,
                    value=_snapshot(top.value),
                    op="complete",
                ),
            )

        self._stack.pop()
        if not self._stack:
            return

        parent = self._stack[-1]
        if parent.type == 1:  # array parent
            # For a nested array closing, use the nested array's own index;
            # for an object element closing, the parent array tracks the index.
            idx = top.index if top.type == 1 else parent.index
            if idx >= 0:
                arr = parent.value
                while len(arr) <= idx:
                    arr.append(None)
                arr[idx] = top.value
        else:  # object parent
            key = (
                top.base_path[len(parent.base_path) + 1:]
                if parent.base_path
                else top.base_path
            )
            parent.value[key] = top.value

    def _pop_key(self) -> None:
        """Close a key frame (primitive value done), emit ``complete``, propagate."""
        if not self._stack:
            return
        top = self._stack[-1]
        if top.type != 0:
            return  # array frame — no-op; primitive array elements bypass key frames

        if self._completions:
            self.emit(
                "patch",
                StreamingChunk(
                    path=self._current_path,
                    value=_snapshot(top.value),
                    op="complete",
                ),
            )

        self._stack.pop()
        if not self._stack:
            return

        parent = self._stack[-1]
        if parent.type == 0:  # propagate to parent object
            key = (
                top.base_path[len(parent.base_path) + 1:]
                if parent.base_path
                else top.base_path
            )
            parent.value[key] = top.value

    def _increment_array_index(self) -> None:
        if not self._stack:
            return
        top = self._stack[-1]
        if top.type != 1:
            return
        top.index = 0 if top.index < 0 else top.index + 1

    # -------------------------------------------------------------------------
    # Patch emission
    # -------------------------------------------------------------------------

    def _patch(self, value: Any, op: str) -> None:
        top = self._stack[-1] if self._stack else None
        if top is not None:
            if op == "add":
                top.value = value
            elif op == "append":
                top.value = (top.value or "") + value

        # For container add patches, emit an empty container rather than the live
        # reference — the Collector builds its own copy from subsequent patches.
        emit_value = value
        if op == "add" and isinstance(value, (dict, list)):
            emit_value = [] if isinstance(value, list) else {}

        self.emit(
            "patch",
            StreamingChunk(path=self._current_path, value=emit_value, op=op),  # type: ignore[arg-type]
        )

    # -------------------------------------------------------------------------
    # Core processing loop — offset-based buffer scan
    # -------------------------------------------------------------------------

    def _process(self) -> None:
        buf = self._buf
        length = len(buf)

        while self._pos < length:
            state = self._state
            if state == _STRING_VALUE:
                self._scan_string_value(buf, length)
            elif state == _OBJECT_KEY_STR:
                self._scan_key_string(buf, length)
            elif state == _UNICODE_ESCAPE:
                # processUnicodeEscape owns pos advancement
                self._state = _STRING_VALUE
                self._process_unicode_escape(buf, length)
            else:
                cc = ord(buf[self._pos])
                self._step_char_code(cc)
                self._pos += 1

    def _scan_string_value(self, buf: str, length: int) -> None:
        """Fast scan for STRING_VALUE — batches plain chars into a single patch."""
        if self._pending_escape:
            self._pending_escape = False
            self._process_escape(buf, length)
            if self._state != _STRING_VALUE:
                return

        while self._pos < length and self._state == _STRING_VALUE:
            seg_start = self._pos

            # Scan to next structural char (quote or backslash)
            while self._pos < length:
                cc = ord(buf[self._pos])
                if cc == _CC_QUOTE or cc == _CC_BACKSLASH:
                    break
                self._pos += 1

            # Emit any plain chars scanned
            if self._pos > seg_start:
                self._emit_string_chunk(buf[seg_start : self._pos])

            if self._pos >= length:
                break

            cc = ord(buf[self._pos])

            if cc == _CC_QUOTE:
                self._pos += 1
                if not self._string_initialised:
                    self._patch("", "add")
                self._pop_key()
                self._state = _AFTER_VALUE
                return

            if cc == _CC_BACKSLASH:
                self._pos += 1
                if self._pos >= length:
                    self._pending_escape = True
                    break
                self._process_escape(buf, length)

    def _scan_key_string(self, buf: str, length: int) -> None:
        """Fast scan for OBJECT_KEY_STRING — accumulates into key_buf."""
        start = self._pos

        while self._pos < length:
            cc = ord(buf[self._pos])

            if cc == _CC_QUOTE:
                self._key_buf += buf[start : self._pos]
                self._pos += 1
                self._state = _AFTER_KEY
                return

            if cc == _CC_BACKSLASH:
                self._key_buf += buf[start : self._pos]
                self._pos += 1
                self._process_escape_into_key_buf(buf, length)
                if self._state == _OBJECT_KEY_STR:
                    self._scan_key_string(buf, length)
                return

            self._pos += 1

        # Partial key — accumulate and wait for next write()
        self._key_buf += buf[start : self._pos]

    def _process_escape(self, buf: str, length: int) -> None:
        """Process one escape sequence (backslash already consumed)."""
        if self._pos >= length:
            return
        cc = ord(buf[self._pos])
        self._pos += 1

        if cc == _CC_u:
            self._process_unicode_escape(buf, length)
            return

        resolved = _ESCAPE_MAP.get(cc, chr(cc))
        self._emit_string_chunk(resolved)

    def _process_unicode_escape(self, buf: str, length: int) -> None:
        """Process \\uXXXX, handling surrogate pairs and cross-token splits."""
        needed = 4 - len(self._unicode_buf)
        available = length - self._pos

        if available < needed:
            self._unicode_buf += buf[self._pos : length]
            self._pos = length
            self._state = _UNICODE_ESCAPE
            return

        self._unicode_buf += buf[self._pos : self._pos + needed]
        self._pos += needed
        code = int(self._unicode_buf, 16)
        self._unicode_buf = ""

        if 0xD800 <= code <= 0xDBFF:
            # High surrogate — wait for the matching low surrogate
            self._high_surrogate = code
            return

        if 0xDC00 <= code <= 0xDFFF and self._high_surrogate:
            # Combine surrogate pair into a supplementary code point
            codepoint = 0x10000 + ((self._high_surrogate - 0xD800) << 10) + (code - 0xDC00)
            self._high_surrogate = 0
            self._emit_string_chunk(chr(codepoint))
            return

        if self._high_surrogate:
            self._emit_string_chunk(chr(self._high_surrogate))
            self._high_surrogate = 0

        self._emit_string_chunk(chr(code))

    def _process_escape_into_key_buf(self, buf: str, length: int) -> None:
        """Same as _process_escape but accumulates into key_buf."""
        if self._pos >= length:
            self._pos -= 1
            return
        cc = ord(buf[self._pos])
        self._pos += 1

        if cc == _CC_u:
            if self._pos + 4 > length:
                # Partial \\uXXXX in key — rare; carry forward raw bytes
                self._key_buf += buf[self._pos - 2 :]
                self._pos = length
                return
            hex_str = buf[self._pos : self._pos + 4]
            self._pos += 4
            self._key_buf += chr(int(hex_str, 16))
            return

        self._key_buf += _ESCAPE_MAP.get(cc, chr(cc))

    def _emit_string_chunk(self, chunk: str) -> None:
        if not self._string_initialised:
            self._patch(chunk, "add")
            self._string_initialised = True
        else:
            self._patch(chunk, "append")

    # -------------------------------------------------------------------------
    # Non-string character dispatch
    # -------------------------------------------------------------------------

    def _step_char_code(self, cc: int) -> None:  # noqa: C901
        state = self._state

        # ── START ──────────────────────────────────────────────────────────────
        if state == _START:
            if cc == _CC_LBRACE:
                self._push_container("", 0)
                if self._root:
                    self._patch({}, "add")
                self._state = _OBJECT_KEY
            elif cc == _CC_LBRACKET:
                self._push_container("", 1)
                if self._root:
                    self._patch([], "add")
                self._state = _ARRAY_VALUE
            # else: whitespace / BOM before root container — ignore

        # ── OBJECT_KEY ─────────────────────────────────────────────────────────
        elif state == _OBJECT_KEY:
            if cc == _CC_QUOTE:
                self._key_buf = ""
                self._state = _OBJECT_KEY_STR
            elif cc == _CC_RBRACE:
                self._pop_stack()
                self._state = _AFTER_VALUE

        # ── AFTER_KEY ──────────────────────────────────────────────────────────
        elif state == _AFTER_KEY:
            if cc == _CC_COLON:
                self._state = _OBJECT_VALUE

        # ── OBJECT_VALUE ───────────────────────────────────────────────────────
        elif state == _OBJECT_VALUE:
            if _is_ws(cc):
                return
            if cc == _CC_QUOTE:
                self._push_key(self._key_buf)
                self._key_buf = ""
                self._string_initialised = False
                self._state = _STRING_VALUE
            elif cc == _CC_LBRACE:
                self._push_container(self._key_buf, 0)
                self._key_buf = ""
                self._patch({}, "add")
                self._state = _OBJECT_KEY
            elif cc == _CC_LBRACKET:
                self._push_container(self._key_buf, 1)
                self._key_buf = ""
                self._patch([], "add")
                self._state = _ARRAY_VALUE
            elif cc == _CC_t or cc == _CC_f:
                self._push_key(self._key_buf)
                self._key_buf = ""
                self._val_buf = chr(cc)
                self._state = _BOOLEAN_VALUE
            elif cc == _CC_n:
                self._push_key(self._key_buf)
                self._key_buf = ""
                self._val_buf = "n"
                self._state = _NULL_VALUE
            elif cc == _CC_MINUS or _CC_0 <= cc <= _CC_9:
                self._push_key(self._key_buf)
                self._key_buf = ""
                self._val_buf = chr(cc)
                self._state = _NUMBER_VALUE

        # ── NUMBER_VALUE ───────────────────────────────────────────────────────
        elif state == _NUMBER_VALUE:
            if (
                _CC_0 <= cc <= _CC_9
                or cc == _CC_DOT
                or cc == _CC_e
                or cc == _CC_E
                or cc == _CC_PLUS
                or cc == _CC_MINUS
            ):
                self._val_buf += chr(cc)
            else:
                self._emit_number()
                self._pop_key()
                self._state = _AFTER_VALUE
                self._step_char_code(cc)  # re-process terminator

        # ── BOOLEAN_VALUE ──────────────────────────────────────────────────────
        elif state == _BOOLEAN_VALUE:
            self._val_buf += chr(cc)
            if self._val_buf == "true":
                self._patch(True, "add")
                self._pop_key()
                self._val_buf = ""
                self._state = _AFTER_VALUE
            elif self._val_buf == "false":
                self._patch(False, "add")
                self._pop_key()
                self._val_buf = ""
                self._state = _AFTER_VALUE

        # ── NULL_VALUE ─────────────────────────────────────────────────────────
        elif state == _NULL_VALUE:
            self._val_buf += chr(cc)
            if self._val_buf == "null":
                self._patch(None, "add")
                self._pop_key()
                self._val_buf = ""
                self._state = _AFTER_VALUE

        # ── ARRAY_VALUE ────────────────────────────────────────────────────────
        elif state == _ARRAY_VALUE:
            if _is_ws(cc) or cc == _CC_COMMA:
                return

            if cc == _CC_RBRACKET:
                self._pop_stack()
                self._state = _AFTER_VALUE
                return

            self._increment_array_index()

            if cc == _CC_QUOTE:
                self._string_initialised = False
                self._state = _STRING_VALUE
            elif cc == _CC_LBRACE:
                self._push_container("", 0)
                self._patch({}, "add")
                self._state = _OBJECT_KEY
            elif cc == _CC_LBRACKET:
                self._push_container("", 1)
                self._patch([], "add")
                self._state = _ARRAY_VALUE
            elif cc == _CC_t or cc == _CC_f:
                self._val_buf = chr(cc)
                self._state = _BOOLEAN_VALUE
            elif cc == _CC_n:
                self._val_buf = "n"
                self._state = _NULL_VALUE
            elif cc == _CC_MINUS or _CC_0 <= cc <= _CC_9:
                self._val_buf = chr(cc)
                self._state = _NUMBER_VALUE

        # ── AFTER_VALUE ────────────────────────────────────────────────────────
        elif state == _AFTER_VALUE:
            if _is_ws(cc):
                return
            if cc == _CC_COMMA:
                top = self._stack[-1] if self._stack else None
                self._state = _ARRAY_VALUE if (top and top.type == 1) else _OBJECT_KEY
            elif cc == _CC_RBRACE or cc == _CC_RBRACKET:
                self._pop_stack()

    # -------------------------------------------------------------------------
    # Finalise — flush pending primitives at end-of-stream
    # -------------------------------------------------------------------------

    def _finalise(self) -> None:
        state = self._state
        if state == _NUMBER_VALUE:
            self._emit_number()
            self._pop_key()
        elif state == _BOOLEAN_VALUE:
            if self._val_buf == "true":
                self._patch(True, "add")
            elif self._val_buf == "false":
                self._patch(False, "add")
            self._pop_key()
            self._val_buf = ""
        elif state == _NULL_VALUE:
            if self._val_buf == "null":
                self._patch(None, "add")
            self._pop_key()
            self._val_buf = ""

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _emit_number(self) -> None:
        raw = self._val_buf
        lower = raw.lower()
        try:
            value: int | float = (
                float(raw) if ("." in lower or "e" in lower) else int(raw, 10)
            )
        except ValueError:
            self.emit("error", JsonCurrentError(f'Invalid number: "{raw}"'))
            self._val_buf = ""
            return
        self._patch(value, "add")
        self._val_buf = ""


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _is_ws(cc: int) -> bool:
    return cc == _CC_SP or cc == _CC_NL or cc == _CC_CR or cc == _CC_TAB


def _snapshot(value: Any) -> Any:
    """Deep-clone a value for safe delivery in ``complete`` patches."""
    if value is None or not isinstance(value, (dict, list)):
        return value
    if isinstance(value, list):
        return [_snapshot(item) for item in value]
    return {k: _snapshot(v) for k, v in value.items()}


_ESCAPE_MAP: dict[int, str] = {
    0x6E: "\n",   # n
    0x74: "\t",   # t
    0x72: "\r",   # r
    0x62: "\b",   # b
    0x66: "\f",   # f
    0x5C: "\\",   # backslash
    0x22: '"',    # quote
    0x2F: "/",    # forward slash
}
