# optimusCapacity — common tasks.
#   make install      create .venv and install
#   make test         run the stub-server test suite
#   make demo         full CP -> RA flow against $(URL)
#   make orphans      reservations issued but never given a CDT

-include .env
export

PY  ?= python3
URL ?= $(or $(OPTIMUSDB_URL),http://localhost:18001)
RA  ?= $(or $(OPTIMUSDB_RA_URL),)
CLI  = $(PY) capacity_client.py --url $(URL)

PROVIDER_ID   ?= did:swarm:cp-iccs-01
CAPACITY_TYPE ?= compute
REGION        ?= eu-gr-athens
EXPECTED_RA   ?= did:swarm:ra-04
CDT           ?= samples/athens_gpu_pool.yaml

.DEFAULT_GOAL := help
.PHONY: help install install-dev test test-cov test-integration lint \
        health stores reserve demo list orphans clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## create .venv and install the client
	./setup.sh

install-dev: ## install with pytest
	./setup.sh --dev

test: ## run the test suite (no agent required)
	$(PY) -m pytest tests/ -q

test-cov: ## run tests with a coverage report
	$(PY) -m pytest tests/ --cov=capacity_client --cov-report=term-missing

test-integration: ## run tests against a live agent
	OPTIMUS_TEST_URL=$(URL) $(PY) -m pytest tests/ -m integration -v

lint: ## byte-compile everything
	$(PY) -m compileall -q capacity_client.py tests/ && echo "ok"

health: ## check the agent and the capacity API
	$(CLI) health

stores: ## list the agent's document stores
	@curl -sS $(URL)/api/v1/stores | $(PY) -m json.tool

reserve: ## issue a capID and print it
	@$(CLI) reserve --provider-id $(PROVIDER_ID) \
	                --capacity-type $(CAPACITY_TYPE) \
	                --region $(REGION) \
	                --expected-ra $(EXPECTED_RA) --quiet

demo: ## full CP -> OptimusDB -> RA flow
	$(CLI) flow --provider-id $(PROVIDER_ID) \
	            --capacity-type $(CAPACITY_TYPE) \
	            --region $(REGION) \
	            --expected-ra $(EXPECTED_RA) \
	            --file $(CDT) \
	            $(if $(RA),--ra-url $(RA),) --cleanup

list: ## list reservations
	$(CLI) list

orphans: ## reservations with no CDT attached
	$(CLI) list --orphans

clean: ## remove build and test artefacts
	rm -rf build dist *.egg-info .pytest_cache .coverage htmlcov
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "cleaned"
