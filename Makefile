.PHONY: help venv install install-dev test demo-dry-run demo-run

PY?=python
VENV=.venv
ACTIVATE=. $(VENV)/bin/activate

help:
	@echo "Targets: venv, install, install-dev, test, demo-dry-run, demo-run"

venv:
	$(PY) -m venv $(VENV)
	$(ACTIVATE); pip install -U pip

install: venv
	$(ACTIVATE); pip install -e .

install-dev: venv
	$(ACTIVATE); pip install -e .[dev]

test:
	$(ACTIVATE); pytest -q

demo-dry-run:
	$(ACTIVATE); transform-service \
		--pipeline-config examples/pipeline.yaml \
		--connection-config examples/connection.yaml \
		--dry-run

demo-run:
	@if [ -z "$$TRINO_JDBC_JAR" ]; then echo "TRINO_JDBC_JAR not set"; exit 1; fi
	bash scripts/run_local.sh examples/pipeline.yaml examples/connection.yaml
