"""Tests for the Emitter FSM — mirrors the JS emitter.test.ts suite."""

from __future__ import annotations

import pytest
from jsoncurrent import Collector, Emitter, StreamingChunk


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def emit_all(json: str, root: str = "") -> list[StreamingChunk]:
    """Feed an entire JSON string in one write."""
    emitter = Emitter(root=root)
    patches: list[StreamingChunk] = []
    emitter.on("patch", patches.append)
    emitter.write(json)
    emitter.flush()
    return patches


def emit_char_by_char(json: str, root: str = "") -> list[StreamingChunk]:
    """Feed a JSON string character-by-character — the hardest case."""
    emitter = Emitter(root=root)
    patches: list[StreamingChunk] = []
    emitter.on("patch", patches.append)
    for char in json:
        emitter.write(char)
    emitter.flush()
    return patches


def data_patches(patches: list[StreamingChunk], path: str) -> list[StreamingChunk]:
    return [p for p in patches if p.path == path and p.op != "complete"]


def assemble_string(patches: list[StreamingChunk], path: str) -> str:
    result = ""
    for p in data_patches(patches, path):
        result = p.value if p.op == "add" else result + p.value
    return result


def assemble(patches: list[StreamingChunk]) -> dict:
    """Reconstruct object from patches using the Collector."""
    collector = Collector()
    for p in patches:
        collector.consume(p)
    collector.complete()
    return collector.value


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

class TestPrimitives:
    def test_string_value(self):
        patches = emit_all('{"title":"Hello"}')
        assert assemble_string(patches, "title") == "Hello"

    def test_integer(self):
        patches = emit_all('{"count":42}')
        assert next(p.value for p in patches if p.path == "count") == 42

    def test_negative_integer(self):
        patches = emit_all('{"n":-7}')
        assert next(p.value for p in patches if p.path == "n") == -7

    def test_float(self):
        patches = emit_all('{"x":3.14}')
        assert next(p.value for p in patches if p.path ==
                    "x") == pytest.approx(3.14)

    def test_true(self):
        patches = emit_all('{"flag":true}')
        assert next(p.value for p in patches if p.path == "flag") is True

    def test_false(self):
        patches = emit_all('{"flag":false}')
        assert next(p.value for p in patches if p.path == "flag") is False

    def test_null(self):
        patches = emit_all('{"val":null}')
        assert next(p.value for p in patches if p.path == "val") is None

    def test_empty_string(self):
        patches = emit_all('{"s":""}')
        add_patches = [p for p in patches if p.path == "s" and p.op == "add"]
        assert len(add_patches) == 1
        assert add_patches[0].value == ""

    def test_number_at_end_of_stream(self):
        """Numbers are finalised by flush() when no trailing delimiter exists."""
        patches = emit_all('{"n":99}')
        assert next(p.value for p in patches if p.path == "n") == 99


# ---------------------------------------------------------------------------
# Nested objects
# ---------------------------------------------------------------------------

class TestNestedObjects:
    def test_nested_object_emits_add_for_container(self):
        patches = emit_all('{"meta":{"id":1}}')
        container_patch = next(
            p for p in patches if p.path == "meta" and p.op == "add")
        assert container_patch.value == {}

    def test_nested_object_field(self):
        patches = emit_all('{"meta":{"id":1}}')
        assert next(p.value for p in patches if p.path == "meta.id") == 1

    def test_full_reconstruction_matches_json(self):
        import json
        src = '{"a":{"b":{"c":"deep"}}}'
        patches = emit_all(src)
        assert assemble(patches) == json.loads(src)


# ---------------------------------------------------------------------------
# Arrays
# ---------------------------------------------------------------------------

class TestArrays:
    def test_array_emits_add_for_container(self):
        patches = emit_all('{"items":[]}')
        container = next(p for p in patches if p.path ==
                         "items" and p.op == "add")
        assert container.value == []

    def test_array_of_strings(self):
        import json
        src = '{"tags":["a","b","c"]}'
        assert assemble(emit_all(src)) == json.loads(src)

    def test_array_of_objects(self):
        import json
        src = '{"cards":[{"term":"hello"},{"term":"world"}]}'
        assert assemble(emit_all(src)) == json.loads(src)

    def test_array_object_element_paths(self):
        patches = emit_all('{"cards":[{"term":"x"}]}')
        assert any(p.path == "cards[0].term" for p in patches)

    def test_array_of_numbers(self):
        import json
        src = '{"nums":[1,2,3]}'
        assert assemble(emit_all(src)) == json.loads(src)


# ---------------------------------------------------------------------------
# Root prefix
# ---------------------------------------------------------------------------

class TestRootPrefix:
    def test_root_prefixes_paths(self):
        patches = emit_all('{"title":"Hi"}', root="prediction")
        assert any(p.path == "prediction" for p in patches)
        assert any(p.path == "prediction.title" for p in patches)

    def test_root_string_assembles_correctly(self):
        patches = emit_all('{"title":"Hello"}', root="prediction")
        assert assemble_string(patches, "prediction.title") == "Hello"

    def test_no_root_no_prefix(self):
        patches = emit_all('{"x":1}')
        assert all("." not in p.path or p.path.startswith("x")
                   for p in patches)


# ---------------------------------------------------------------------------
# Streaming (character-by-character)
# ---------------------------------------------------------------------------

class TestStreaming:
    def test_string_char_by_char(self):
        patches = emit_char_by_char('{"title":"Hello World"}')
        assert assemble_string(patches, "title") == "Hello World"

    def test_number_char_by_char(self):
        patches = emit_char_by_char('{"n":42}')
        assert assemble(patches) == {"n": 42}

    def test_nested_object_char_by_char(self):
        import json
        src = '{"a":{"b":"c"}}'
        assert assemble(emit_char_by_char(src)) == json.loads(src)

    def test_array_char_by_char(self):
        import json
        src = '{"cards":[{"term":"hi"}]}'
        assert assemble(emit_char_by_char(src)) == json.loads(src)


# ---------------------------------------------------------------------------
# Escape sequences
# ---------------------------------------------------------------------------

class TestEscapes:
    def test_newline_escape(self):
        patches = emit_all('{"s":"a\\nb"}')
        assert assemble_string(patches, "s") == "a\nb"

    def test_tab_escape(self):
        patches = emit_all('{"s":"a\\tb"}')
        assert assemble_string(patches, "s") == "a\tb"

    def test_unicode_escape(self):
        patches = emit_all('{"s":"\\u0041"}')  # 'A'
        assert assemble_string(patches, "s") == "A"

    def test_unicode_surrogate_pair(self):
        # 𠜎 = U+2070E, encoded as surrogate pair \uD841\uDF0E
        patches = emit_all('{"s":"\\uD841\\uDF0E"}')
        assert assemble_string(patches, "s") == "\U0002070E"

    def test_escaped_quote_in_key(self):
        patches = emit_all('{"a\\"b":1}')
        assert assemble(patches) == {'a"b': 1}

    def test_back_to_back_escapes(self):
        patches = emit_all('{"s":"\\\\"}')  # single backslash
        assert assemble_string(patches, "s") == "\\"


# ---------------------------------------------------------------------------
# Completions
# ---------------------------------------------------------------------------

class TestCompletions:
    def test_complete_patches_emitted_by_default(self):
        patches = emit_all('{"title":"Hi"}')
        assert any(p.op == "complete" for p in patches)

    def test_complete_disabled(self):
        emitter = Emitter(completions=False)
        patches: list[StreamingChunk] = []
        emitter.on("patch", patches.append)
        emitter.write('{"title":"Hi"}')
        emitter.flush()
        assert not any(p.op == "complete" for p in patches)

    def test_complete_fires_after_flush(self):
        emitter = Emitter()
        done = []
        emitter.on("complete", lambda: done.append(True))
        emitter.write('{"x":1}')
        emitter.flush()
        assert done == [True]


# ---------------------------------------------------------------------------
# Multi-token splitting
# ---------------------------------------------------------------------------

class TestMultiToken:
    def test_key_split_across_tokens(self):
        emitter = Emitter()
        patches: list[StreamingChunk] = []
        emitter.on("patch", patches.append)
        emitter.write('{"hel')
        emitter.write('lo":"world"}')
        emitter.flush()
        assert assemble_string(patches, "hello") == "world"

    def test_value_split_across_tokens(self):
        emitter = Emitter()
        patches: list[StreamingChunk] = []
        emitter.on("patch", patches.append)
        emitter.write('{"msg":"hel')
        emitter.write('lo"}')
        emitter.flush()
        assert assemble_string(patches, "msg") == "hello"

    def test_number_split_across_tokens(self):
        emitter = Emitter()
        patches: list[StreamingChunk] = []
        emitter.on("patch", patches.append)
        emitter.write('{"n":12')
        emitter.write('34}')
        emitter.flush()
        assert assemble(patches) == {"n": 1234}

    def test_unicode_split_across_tokens(self):
        emitter = Emitter()
        patches: list[StreamingChunk] = []
        emitter.on("patch", patches.append)
        # Split \u0041 across two writes
        emitter.write('{"s":"\\u00')
        emitter.write('41"}')
        emitter.flush()
        assert assemble_string(patches, "s") == "A"


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

class TestReset:
    def test_reset_allows_reuse(self):
        emitter = Emitter()
        patches: list[StreamingChunk] = []
        emitter.on("patch", patches.append)

        emitter.write('{"a":1}')
        emitter.flush()
        first = [p for p in patches]

        patches.clear()
        emitter.write('{"b":2}')
        emitter.flush()
        second = [p for p in patches]

        assert any(p.path == "a" for p in first)
        assert any(p.path == "b" for p in second)
        assert not any(p.path == "a" for p in second)
