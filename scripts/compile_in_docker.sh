#!/usr/bin/env bash

# exit now if shell command return not zero in this scripts;
set -e;

PROJECT_HOME=$(cd "$(dirname "${0}")/.." || exit 1; pwd)

# You can set the `SERVICE_NAME` by export SERVICE_NAME="xxx".
if [ -z "${SERVICE_NAME}" ]; then
  SERVICE_NAME=$(basename "${PROJECT_HOME}")
fi

# switch the workdir to project home.
cd "${PROJECT_HOME}"

BUILD_DIR="./build/docker"
COMPILE_IMAGE="dataomnis/${SERVICE_NAME}-builder:$(cat ./scripts/docker/compile.version)"

# Build go-builder image if not exists.
if [ "$(docker image ls "${COMPILE_IMAGE}" | wc -l | awk '{print $1}')" -eq "1" ]; then
  docker build -t "${COMPILE_IMAGE}" -f ./scripts/docker/compile.Dockerfile .
fi

LOCAL_CACHE="$(go env GOCACHE)"
LOCAL_MOD_CACHE="$(go env GOPATH)"/pkg

# Compile go program.
docker run --rm \
-v "${PROJECT_HOME}":/opt/dataomnis \
-v "${LOCAL_CACHE}":/go/cache \
-v "${LOCAL_MOD_CACHE}":/go/pkg \
-w /opt/dataomnis \
"${COMPILE_IMAGE}" \
bash -c "sh -x ./scripts/compile.sh -s ${SERVICE_NAME} -o ${BUILD_DIR}"
