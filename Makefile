.PHONY: venv install format lint typecheck build test test-unit test-integration clean

VENV ?= .venv
PYTHON ?= $(VENV)/bin/python

# The venv is created with --system-site-packages so it picks up an
# already-installed ruff/mypy/pytest/hatchling when the package index is
# unreachable; `pip install -e .` still wins when the network is available.
venv:
	python3 -m venv --system-site-packages $(VENV)

install: venv
	$(PYTHON) -m pip install -e ".[dev]" || true

format:
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

typecheck:
	$(PYTHON) -m mypy rabbitmq_amqp_python_client
	MYPYPATH=. $(PYTHON) -m mypy docs/examples

build:
	$(PYTHON) -m build

test: test-unit test-integration

test-unit:
	$(PYTHON) -m pytest tests/unit -v

test-integration:
	$(PYTHON) -m pytest tests/integration -v -m integration

clean:
	rm -rf $(VENV) build dist *.egg-info .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
