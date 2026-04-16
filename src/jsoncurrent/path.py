from __future__ import annotations

import re
from typing import Any


def parse_path(path: str) -> list[str | int]:
    """
    Parse a dot-notation path string with optional bracket indices into a key list.

    Examples::

        parse_path("title")           # ["title"]
        parse_path("meta.createdAt")  # ["meta", "createdAt"]
        parse_path("sections[0]")     # ["sections", 0]
        parse_path("sections[2].heading")  # ["sections", 2, "heading"]
        parse_path("[0]")             # [0]
        parse_path("")                # []
    """
    if not path:
        return []

    parts: list[str | int] = []
    for segment in path.split("."):
        # Bare array index: e.g. "[0]" from a root-level array path
        index_only = re.match(r"^\[(\d+)\]$", segment)
        if index_only:
            parts.append(int(index_only.group(1)))
            continue

        # Key + optional bracket index: e.g. "sections[0]"
        bracket_idx = segment.find("[")
        if bracket_idx == -1:
            parts.append(segment)
        else:
            key = segment[:bracket_idx]
            if key:
                parts.append(key)
            for m in re.finditer(r"\[(\d+)\]", segment[bracket_idx:]):
                parts.append(int(m.group(1)))

    return parts


def get_path(obj: Any, path: str, fallback: Any = None) -> Any:
    """
    Traverse *obj* following *path*, returning *fallback* if any step is missing.

    Returns ``None`` (not fallback) when the value at *path* is explicitly ``None``.
    """
    if not path:
        return obj if obj is not None else fallback

    cur: Any = obj
    for key in parse_path(path):
        try:
            cur = cur[key]
        except (KeyError, IndexError, TypeError):
            return fallback
    return cur


def set_path(obj: Any, path: str, value: Any) -> None:
    """
    Set *value* at *path* inside *obj*, creating intermediate dicts or lists as needed.

    Mutates *obj* in place. Integer path segments imply list intermediates;
    string segments imply dict intermediates.
    """
    keys = parse_path(path)
    if not keys:
        return

    cur: Any = obj
    for i in range(len(keys) - 1):
        key = keys[i]
        next_key = keys[i + 1]
        intermediate: Any = [] if isinstance(next_key, int) else {}

        if isinstance(key, int):
            # Extend list to accommodate the index
            while len(cur) <= key:
                cur.append(None)
            if cur[key] is None:
                cur[key] = intermediate
            cur = cur[key]
        else:
            existing = cur.get(key)  # type: ignore[union-attr]
            if existing is None:
                cur[key] = intermediate  # type: ignore[index]
            cur = cur[key]  # type: ignore[index]

    last = keys[-1]
    if isinstance(last, int):
        if isinstance(cur, list):
            while len(cur) <= last:
                cur.append(None)
        cur[last] = value
    else:
        cur[last] = value  # type: ignore[index]
