#!/usr/bin/env bash

# exit now if shell command return not zero in this scripts;
set -e;

PROJECT_HOME=$(cd "$(dirname "${0}")/.." || exit 1; pwd)

# You can set the `SERVICE_NAME` by export SERVICE_NAME="xxx".
if [ -z "${SERVICE_NAME}" ]; then
  SERVICE_NAME=$(basename "${PROJECT_HOME}")
fi

sh -x ./scripts/compile.sh -s "${SERVICE_NAME}"
