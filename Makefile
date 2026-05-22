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
	uv run pytest tests/ -v $(EXTRA_ARGS)

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
