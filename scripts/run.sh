#!/usr/bin/env bash

# exit now if shell command return not zero in this scripts;
set -e;

CURRENT_PATH=$(cd "$(dirname "${0}")" || exit 1; pwd)
cd "${CURRENT_PATH}"/..

go run -race main.go -c config/config.yaml start
