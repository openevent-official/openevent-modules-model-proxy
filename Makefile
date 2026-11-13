PYTHON ?= python3
TEST_ARGS ?=
INSTALL_ARGS ?=
CHECK_API_ARGS ?=

.PHONY: help build install test e2e check-api clean

help:
	@printf 'Targets: build install test e2e check-api clean\n'
	@printf 'Use TEST_ARGS to select unittest tests; OPENEVENT_SERVER_BIN selects the e2e server.\n'
	@printf 'Use CHECK_API_ARGS="--config model-proxy.yaml --model MODEL" for check-api.\n'

build:
	PYTHON="$(PYTHON)" ./build.sh

install: build
	"$(PYTHON)" -B scripts/install.py $(INSTALL_ARGS)

test:
	PYTHON="$(PYTHON)" ./test.sh $(TEST_ARGS)

e2e:
	PYTHON="$(PYTHON)" ./test-e2e.sh $(TEST_ARGS)

check-api:
	PYTHON="$(PYTHON)" ./check-api.sh $(CHECK_API_ARGS)

clean:
	rm -rf build dist
