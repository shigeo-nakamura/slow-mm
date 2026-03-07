#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." &> /dev/null && pwd)"

if [ -z "$1" ]; then
    echo "Error: No environment argument provided."
    exit 1
fi

# Resolve MM YAML config path.
if [ -z "${MM_CONFIG_PATH:-}" ]; then
    MM_CONFIG_PATH="${BASE_DIR}/configs/mm/$1.yaml"
    export MM_CONFIG_PATH
fi
if [ ! -f "$MM_CONFIG_PATH" ]; then
    echo "Error: MM config not found: $MM_CONFIG_PATH"
    exit 1
fi

ENV_DIR="${SLOW_MM_ENV_DIR:-${BASE_DIR}/scripts}"
ENV_FILE="${ENV_DIR}/$1.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: Env file not found: $ENV_FILE"
    exit 1
fi

source "$ENV_FILE"

# Ensure lighter-go shared library is discoverable
if [ -z "${LIGHTER_GO_PATH:-}" ] && [ -f "${BASE_DIR}/lib/libsigner.so" ]; then
    export LIGHTER_GO_PATH="${BASE_DIR}/lib"
fi
if [ -n "${LIGHTER_GO_PATH:-}" ]; then
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

exec "${BASE_DIR}/bin/slow-mm"
