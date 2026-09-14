#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONDONTWRITEBYTECODE=1
TASK_PYTHON="${PYTHON:-python3}"
mkdir -p build/tests
export TMPDIR="$PWD/build/tests"
export PYTHONPATH="$PWD/src:$PWD/tests${PYTHONPATH:+:$PYTHONPATH}"
if [[ $# -gt 0 ]]; then
    exec "$TASK_PYTHON" -B -m unittest -v "$@"
fi
exec "$TASK_PYTHON" -B -m unittest discover -s tests -p 'test_*.py' -v
