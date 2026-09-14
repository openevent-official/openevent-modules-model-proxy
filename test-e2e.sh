#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONDONTWRITEBYTECODE=1
TASK_PYTHON="${PYTHON:-python3}"
"$TASK_PYTHON" -B - <<'PY'
import importlib.metadata
try:
    version = importlib.metadata.version('openevent-sdk')
except importlib.metadata.PackageNotFoundError:
    raise SystemExit('openevent-sdk>=0.8.0 must already be installed') from None
try:
    from packaging.specifiers import SpecifierSet
except ImportError:
    raise SystemExit('Install the packaging test dependency before running e2e; see README.md') from None
if not SpecifierSet('>=0.8.0').contains(version, prereleases=True):
    raise SystemExit(f'openevent-sdk>=0.8.0 must already be installed; found {version}')
from openevent.sdk import OpenEventClient
PY
TASK_SERVER="${OPENEVENT_SERVER_BIN:-}"
if [[ -z "$TASK_SERVER" || ! -x "$TASK_SERVER" ]]; then
    printf 'Set OPENEVENT_SERVER_BIN to an executable OpenEvent server.\n' >&2
    exit 1
fi
export OPENEVENT_SERVER_BIN="$TASK_SERVER"
make build PYTHON="$TASK_PYTHON"
rm -rf build/e2e-package
"$TASK_PYTHON" -B -m pip install --no-deps --target build/e2e-package dist/openevent_model_proxy-*.whl
mkdir -p build/e2e
export TMPDIR="$PWD/build/e2e"
export PYTHONPATH="$PWD/build/e2e-package:$PWD/tests"
export OPENEVENT_RUN_E2E=1
if [[ $# -gt 0 ]]; then
    exec timeout --kill-after=10s "${E2E_TIMEOUT_SECONDS:-120}s" "$TASK_PYTHON" -B -m unittest -v "$@"
fi
exec timeout --kill-after=10s "${E2E_TIMEOUT_SECONDS:-120}s" "$TASK_PYTHON" -B -m unittest -v test_e2e test_fault_e2e
