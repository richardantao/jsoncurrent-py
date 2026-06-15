"""Tests for the Collector — mirrors the JS collector.test.ts suite."""

from __future__ import annotations

import pytest
import json

from jsoncurrent import Collector, Emitter, JsonCurrentError, StreamingChunk


# ---------------------------------------------------------------------------
# Basic ops
# ---------------------------------------------------------------------------

class TestCollectorBasicOps:
    def test_add_op(self):
        c: Collector = Collector()
        c.consume(StreamingChunk(path="title", value="Hello", op="add"))
        assert c.value == {"title": "Hello"}

    def test_append_op(self):
        c: Collector = Collector()
        c.consume(StreamingChunk(path="title", value="Hel", op="add"))
        c.consume(StreamingChunk(path="title", value="lo", op="append"))
        assert c.value == {"title": "Hello"}

    def test_insert_op(self):
        c: Collector = Collector()
        c.consume(StreamingChunk(path="items", value=[], op="add"))
        c.consume(StreamingChunk(path="items", value="a", op="insert"))
        c.consume(StreamingChunk(path="items", value="b", op="insert"))
        assert c.value == {"items": ["a", "b"]}

    def test_complete_op_does_not_mutate_state(self):
        c: Collector = Collector()
        c.consume(StreamingChunk(path="x", value=1, op="add"))
        before = c.value.copy()
        c.consume(StreamingChunk(path="x", value=1, op="complete"))
        assert c.value == before

    def test_nested_add(self):
        c: Collector = Collector()
        c.consume(StreamingChunk(path="meta", value={}, op="add"))
        c.consume(StreamingChunk(path="meta.id", value=1, op="add"))
        assert c.value == {"meta": {"id": 1}}


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

class TestCollectorEvents:
    def test_change_event_fires_after_each_patch(self):
        changes = []
        c: Collector = Collector()
        c.on("change", lambda state, path, op: changes.append((dict(state), path, op)))
        c.consume(StreamingChunk(path="x", value=1, op="add"))
        c.consume(StreamingChunk(path="x", value=2, op="add"))
        assert len(changes) == 2
        assert changes[-1][0] == {"x": 2}
        assert changes[-1][1] == "x"
        assert changes[-1][2] == "add"

    def test_change_event_state_is_shallow_copy(self):
        snapshots = []
        c: Collector = Collector()
        c.on("change", lambda s, _path, _op: snapshots.append(dict(s)))
        c.consume(StreamingChunk(path="x", value=1, op="add"))
        c.consume(StreamingChunk(path="x", value=2, op="add"))
        assert snapshots[0]["x"] == 1
        assert snapshots[1]["x"] == 2

    def test_complete_event_fires(self):
        results = []
        c: Collector = Collector()
        c.on("complete", results.append)
        c.consume(StreamingChunk(path="x", value=1, op="add"))
        c.complete()
        assert len(results) == 1
        assert results[0] == {"x": 1}

    def test_complete_idempotent(self):
        results = []
        c: Collector = Collector()
        c.on("complete", results.append)
        c.complete()
        c.complete()
        assert len(results) == 1

    def test_pathstart_fires_on_first_add(self):
        starts = []
        c: Collector = Collector()
        c.on("pathstart", lambda path, val: starts.append((path, val)))
        c.consume(StreamingChunk(path="title", value="Hi", op="add"))
        c.consume(StreamingChunk(path="title", value="!", op="append"))
        assert len(starts) == 1
        assert starts[0][0] == "title"

    def test_pathcomplete_fires_on_complete_op(self):
        completions = []
        c: Collector = Collector()
        c.on("pathcomplete", lambda path, val: completions.append((path, val)))
        c.consume(StreamingChunk(path="title", value="Hi", op="add"))
        c.consume(StreamingChunk(path="title", value="Hi", op="complete"))
        assert len(completions) == 1
        assert completions[0] == ("title", "Hi")

    def test_error_event_fires_on_unknown_op(self):
        errors = []
        c: Collector = Collector()
        c.on("error", errors.append)
        c.consume(StreamingChunk(path="x", value=1, op="add"))  # seed state
        # Manually craft an unknown op to trigger the error path
        # type: ignore[arg-type]
        bad = StreamingChunk(path="x", value=1, op="unknown")
        c.consume(bad)
        assert len(errors) == 1
        assert isinstance(errors[0], JsonCurrentError)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

class TestMiddleware:
    def test_middleware_can_transform_patch(self):
        c: Collector = Collector()

        def upper_middleware(patch, next_fn):
            if isinstance(patch.value, str):
                next_fn(StreamingChunk(path=patch.path,
                        value=patch.value.upper(), op=patch.op))
            else:
                next_fn(patch)

        c.use(upper_middleware)
        c.consume(StreamingChunk(path="title", value="hello", op="add"))
        assert c.value == {"title": "HELLO"}

    def test_middleware_can_duplicate_patch(self):
        c: Collector = Collector()

        def mirror(patch, next_fn):
            next_fn(patch)
            if patch.path.endswith(".term"):
                next_fn(StreamingChunk(
                    path=patch.path.replace(".term", ".original_term"),
                    value=patch.value,
                    op=patch.op,
                ))

        c.use(mirror)
        c.consume(StreamingChunk(path="card.term", value="Mitosis", op="add"))
        assert c.value.get("card", {}).get("term") == "Mitosis"
        assert c.value.get("card", {}).get("original_term") == "Mitosis"

    def test_middleware_can_drop_patch(self):
        c: Collector = Collector()

        def drop_private(patch, next_fn):
            if not patch.path.startswith("_"):
                next_fn(patch)

        c.use(drop_private)
        c.consume(StreamingChunk(path="_secret", value="hidden", op="add"))
        c.consume(StreamingChunk(path="title", value="visible", op="add"))
        assert "_secret" not in c.value
        assert c.value["title"] == "visible"

    def test_middleware_chain_order(self):
        calls = []
        c: Collector = Collector()

        def mw1(patch, next_fn):
            calls.append("mw1")
            next_fn(patch)

        def mw2(patch, next_fn):
            calls.append("mw2")
            next_fn(patch)

        c.use(mw1)
        c.use(mw2)
        c.consume(StreamingChunk(path="x", value=1, op="add"))
        assert calls == ["mw1", "mw2"]


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

class TestCollectorReset:
    def test_reset_clears_state(self):
        c: Collector = Collector()
        c.consume(StreamingChunk(path="x", value=1, op="add"))
        c.reset()
        assert c.value == {}

    def test_reset_allows_reuse(self):
        c: Collector = Collector()
        c.consume(StreamingChunk(path="a", value=1, op="add"))
        c.complete()
        c.reset()
        assert not c.is_complete
        c.consume(StreamingChunk(path="b", value=2, op="add"))
        assert c.value == {"b": 2}

    def test_reset_preserves_middleware(self):
        c: Collector = Collector()
        calls = []
        c.use(lambda p, n: (calls.append(1), n(p)))
        c.reset()
        c.consume(StreamingChunk(path="x", value=1, op="add"))
        assert calls == [1]

    def test_consume_after_complete_raises(self):
        c: Collector = Collector()
        c.complete()
        with pytest.raises(JsonCurrentError):
            c.consume(StreamingChunk(path="x", value=1, op="add"))


# ---------------------------------------------------------------------------
# End-to-end: Emitter → Collector
# ---------------------------------------------------------------------------

class TestEmitterCollectorIntegration:
    def _round_trip(self, json_str: str, root: str = "") -> dict:
        emitter = Emitter(root=root)
        collector: Collector = Collector()
        emitter.on("patch", collector.consume)
        emitter.write(json_str)
        emitter.flush()
        collector.complete()
        return collector.value

    def test_flat_object(self):
        src = '{"title":"Hello","count":3,"flag":true}'
        result = self._round_trip(src)
        assert result == json.loads(src)

    def test_nested_object(self):
        src = '{"a":{"b":{"c":42}}}'
        assert self._round_trip(src) == json.loads(src)

    def test_array_of_objects(self):
        src = '{"cards":[{"term":"a","def":"b"},{"term":"c","def":"d"}]}'
        assert self._round_trip(src) == json.loads(src)

    def test_with_root_prefix(self):
        src = '{"title":"Hi"}'
        result = self._round_trip(src, root="prediction")
        assert result == {"prediction": {"title": "Hi"}}

    def test_streaming_string(self):
        emitter = Emitter()
        collector: Collector = Collector()
        changes = []
        collector.on("change", lambda s, _path, _op: changes.append(dict(s)))
        emitter.on("patch", collector.consume)

        # Simulate token-by-token streaming
        emitter.write('{"msg":"')
        emitter.write("Hel")
        emitter.write("lo")
        emitter.write('"}')
        emitter.flush()
        collector.complete()

        # Intermediate states should have partial values
        partial_titles = [c.get("msg", "") for c in changes]
        assert "Hel" in partial_titles or any(
            "Hel" in v for v in partial_titles)
        assert collector.value == {"msg": "Hello"}

    def test_change_fires_for_each_token(self):
        emitter = Emitter()
        collector: Collector = Collector()
        changes = []
        collector.on("change", lambda state, path, op: changes.append((dict(state), path, op)))
        emitter.on("patch", collector.consume)

        emitter.write('{"msg":"Hel')
        emitter.write('lo"}')
        emitter.flush()

        # At least two changes: one for first string token, one for append
        assert len(changes) >= 2
        assert all(len(change) == 3 for change in changes)
        assert collector.value == {"msg": "Hello"}
