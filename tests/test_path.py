"""Tests for parse_path, get_path, set_path."""

from jsoncurrent import get_path, parse_path, set_path


# ---------------------------------------------------------------------------
# parse_path
# ---------------------------------------------------------------------------

class TestParsePath:
    def test_simple_key(self):
        assert parse_path("title") == ["title"]

    def test_nested_keys(self):
        assert parse_path("meta.createdAt") == ["meta", "createdAt"]

    def test_array_index(self):
        assert parse_path("cards[0]") == ["cards", 0]

    def test_nested_key_inside_array_element(self):
        assert parse_path("cards[2].term") == ["cards", 2, "term"]

    def test_deeply_nested_path(self):
        assert parse_path("a.b[1].c.d[0]") == ["a", "b", 1, "c", "d", 0]

    def test_empty_path(self):
        assert parse_path("") == []

    def test_bare_array_index(self):
        # Root-level array path
        assert parse_path("[0]") == [0]

    def test_multiple_array_indices(self):
        assert parse_path("matrix[0][1]") == ["matrix", 0, 1]


# ---------------------------------------------------------------------------
# get_path
# ---------------------------------------------------------------------------

class TestGetPath:
    def test_top_level_key(self):
        assert get_path({"title": "Hello"}, "title") == "Hello"

    def test_nested_key(self):
        assert get_path({"meta": {"id": 1}}, "meta.id") == 1

    def test_array_element(self):
        assert get_path({"cards": ["a", "b"]}, "cards[0]") == "a"

    def test_key_inside_array_element(self):
        assert get_path({"cards": [{"term": "Mitosis"}]},
                        "cards[0].term") == "Mitosis"

    def test_missing_key_returns_fallback(self):
        assert get_path({}, "missing", "default") == "default"

    def test_missing_nested_returns_fallback(self):
        assert get_path({"a": {}}, "a.b.c", None) is None

    def test_none_value_returned_not_fallback(self):
        assert get_path({"x": None}, "x", "fallback") is None

    def test_empty_path_returns_obj(self):
        obj = {"a": 1}
        assert get_path(obj, "") is obj


# ---------------------------------------------------------------------------
# set_path
# ---------------------------------------------------------------------------

class TestSetPath:
    def test_top_level_key(self):
        obj: dict = {}
        set_path(obj, "title", "Hello")
        assert obj == {"title": "Hello"}

    def test_nested_key_creates_intermediates(self):
        obj: dict = {}
        set_path(obj, "meta.createdAt", "2025-01-01")
        assert obj == {"meta": {"createdAt": "2025-01-01"}}

    def test_array_element(self):
        obj: dict = {}
        set_path(obj, "cards", [])
        set_path(obj, "cards[0]", {"term": ""})
        assert obj["cards"][0] == {"term": ""}

    def test_key_inside_array_element(self):
        obj: dict = {}
        set_path(obj, "cards", [{}])
        set_path(obj, "cards[0].term", "Mitosis")
        assert get_path(obj, "cards[0].term") == "Mitosis"

    def test_empty_path_is_noop(self):
        obj = {"a": 1}
        set_path(obj, "", "ignored")
        assert obj == {"a": 1}

    def test_creates_list_for_integer_key(self):
        obj: dict = {}
        set_path(obj, "items[0]", "first")
        assert obj["items"] == ["first"]

    def test_extends_list_for_non_contiguous_index(self):
        obj: dict = {"items": ["a"]}
        set_path(obj, "items[2]", "c")
        assert obj["items"][0] == "a"
        assert obj["items"][1] is None
        assert obj["items"][2] == "c"
