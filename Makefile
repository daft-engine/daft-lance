.DEFAULT_GOAL := help

SHELL := /bin/bash
VENV := .venv

.PHONY: sync
sync:  ## Sync development environment
	@command -v uv > /dev/null || (echo "Error: uv is required but not installed. Please install uv first." && exit 1)
	uv sync --all-groups

.PHONY: hooks
hooks: sync  ## Install pre-commit hooks
	uvx pre-commit install --install-hooks

.PHONY: build
build: sync  ## Build sdist and wheel packages
	uv build

.PHONY: test
test: sync  ## Run tests
	# RAY_ENABLE_UV_RUN_RUNTIME_ENV=0: Ray walks the driver's process tree, sees
	# the `uv run` below, and then replicates that environment to every worker --
	# packaging the whole working directory and rebuilding the venv per worker.
	# Under DAFT_RUNNER=ray that makes worker startup fail once the suite has
	# created enough actors: 277 tests died with ActorUnschedulableError, and the
	# same suite passes in 46s with it off. Workers inherit this venv's
	# interpreter from the raylet regardless.
	DAFT_ANALYTICS_ENABLED=0 RAY_ENABLE_UV_RUN_RUNTIME_ENV=0 uv run pytest tests/ -v $(EXTRA_ARGS)

.PHONY: bench
bench: sync  ## Measure lance session memory in a real Ray run
	DAFT_ANALYTICS_ENABLED=0 uv run --group ray python benchmarks/lance_session_memory.py $(EXTRA_ARGS)

.PHONY: format
format: sync  ## Format Python code
	uv run ruff format daft_lance tests
	uv run ruff check --fix daft_lance tests

.PHONY: lint
lint: sync  ## Lint Python code
	uv run ruff check daft_lance tests

.PHONY: typecheck
typecheck: sync  ## Run mypy type checking
	uv run mypy daft_lance/ tests/

.PHONY: check-format
check-format: sync  ## Check formatting without modifying files
	uv run ruff format --check daft_lance tests
	uv run ruff check daft_lance tests

.PHONY: precommit
precommit: sync  ## Run all pre-commit hooks
	uvx pre-commit run --all-files --hook-stage pre-commit --hook-stage manual

.PHONY: clean
clean:  ## Remove virtual environment and build artifacts
	rm -rf $(VENV)
	rm -rf build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +

.PHONY: help
help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
