#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
TASK_PYTHON="${PYTHON:-python3}"
export PYTHONDONTWRITEBYTECODE=1
mkdir -p build/tools dist
rm -f dist/openevent_model_proxy-*.whl
if ! PYTHONPATH="$PWD/build/tools${PYTHONPATH:+:$PYTHONPATH}" "$TASK_PYTHON" -B -c 'import hatchling' 2>/dev/null; then
    "$TASK_PYTHON" -B -m pip install --target build/tools 'hatchling>=1.26'
fi
PYTHONPATH="$PWD/build/tools${PYTHONPATH:+:$PYTHONPATH}" "$TASK_PYTHON" -B -m hatchling build -t wheel
