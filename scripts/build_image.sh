#!/usr/bin/env bash

# exit now if shell command return not zero in this scripts;
set -e;

PROJECT_HOME=$(cd "$(dirname "${0}")/.." || exit 1; pwd)

# You can set the `SERVICE_NAME` by export SERVICE_NAME="xxx".
if [ -z "${SERVICE_NAME}" ]; then
  SERVICE_NAME=$(basename "${PROJECT_HOME}")
fi
# You can set the `IMAGE_NAME` by export IMAGE_NAME="xxx".
if [ -z "${IMAGE_NAME}" ]; then
  IMAGE_NAME="dataomnis/${SERVICE_NAME}"
fi
# You can set the `IMAGE_TAG` by export IMAGE_TAG="xxx".
if [ -z "${IMAGE_TAG}" ]; then
  IMAGE_TAG=$(git --git-dir="${PROJECT_HOME}/.git" --work-tree="${PROJECT_HOME}" rev-parse --abbrev-ref HEAD | sed -e "s@^heads/@@")
fi

# switch the workdir to project home.
cd "${PROJECT_HOME}"

COMPILE_IMAGE="dataomnis/${SERVICE_NAME}-builder:$(cat ./scripts/docker/compile.version)"
BUILD_DIR="./build/docker"
SERVICE_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

if [ ! -d "${BUILD_DIR}" ]; then
  echo "ERROR: build directory <${BUILD_DIR}> not found, please compile first"
  exit 1
fi
if [ ! -f "${BUILD_DIR}/${SERVICE_NAME}" ]; then
  echo "ERROR: binary program <${BUILD_DIR}/${SERVICE_NAME}> not found, please compile first"
fi

cp -frp ./scripts/docker/build.Dockerfile "${BUILD_DIR}"/Dockerfile
cp -frp ./config/config.yaml "${BUILD_DIR}/${SERVICE_NAME}.yaml"

# Build docker image for services
docker build --build-arg SERVICE="${SERVICE_NAME}" --build-arg COMPILE_IMAGE="${COMPILE_IMAGE}" -t "${SERVICE_IMAGE}" "${BUILD_DIR}"
