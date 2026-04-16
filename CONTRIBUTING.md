# Contributing

Thanks for contributing to jsoncurrent.

## Development setup

This project uses:

- setuptools for packaging
- uv for local environment management and command execution

### Prerequisites

- Python 3.10+
- uv installed

Install uv (one-time):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Install project dependencies (including dev tools):

```bash
uv sync --extra dev
```

## Common commands

Run lint:

```bash
uv run ruff check .
```

Run tests:

```bash
uv run pytest
```

Build distributions:

```bash
uv build
```

Verify built distributions:

```bash
uv run twine check dist/*
```

## Project layout

- src/jsoncurrent: package source
- tests: test suite

## Release notes

- CI and publish workflows use uv for dependency installation and build steps.
- PyPI publishing is handled by GitHub Actions on version tags.
