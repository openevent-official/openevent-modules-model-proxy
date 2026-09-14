PYTHON ?= python3
TEST_ARGS ?=
INSTALL_ARGS ?=

.PHONY: help build install test e2e clean

help:
	@printf 'Targets: build install test e2e clean\n'
	@printf 'Use TEST_ARGS to select unittest tests; OPENEVENT_SERVER_BIN selects the e2e server.\n'

build:
	PYTHON="$(PYTHON)" ./build.sh

install: build
	"$(PYTHON)" -B -m pip install $(INSTALL_ARGS) dist/openevent_model_proxy-*.whl

test:
	PYTHON="$(PYTHON)" ./test.sh $(TEST_ARGS)

e2e:
	PYTHON="$(PYTHON)" ./test-e2e.sh $(TEST_ARGS)

clean:
	rm -rf build dist
