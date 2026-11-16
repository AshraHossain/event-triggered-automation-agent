.PHONY: install check lint type security test demo benchmark

install:
	uv venv && uv pip install -e ".[dev]"

# One command to verify everything CI verifies.
check: lint type security test

lint:
	uv run ruff check .
	uv run black --check .

type:
	uv run mypy etaa

security:
	uv run bandit -q -r etaa
	uv run pip-audit --skip-editable

test:
	uv run pytest -q --cov=etaa --cov-report=term-missing --cov-fail-under=90

demo:
	PYTHONPATH=. uv run python examples/payment_webhook/demo.py

benchmark:
	uv run python benchmark.py --iterations 20
