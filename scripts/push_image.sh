#!/usr/bin/env bash

# exit now if shell command return not zero in this scripts;
set -e;

PROJECT_HOME=$(cd "$(dirname "${0}")/.." || exit 1; pwd)

# You can set the `IMAGE_NAME` by export SERVICE_NAME="xxx".
if [ -z "${SERVICE_NAME}" ]; then
  SERVICE_NAME=$(basename "${PROJECT_HOME}")
fi
# You can set the `IMAGE_NAME` by export CENTER_REPO="xxx".
if [ -z "${CENTER_REPO}" ]; then
  CENTER_REPO="dockerhub.dev.data.qingcloud.link"
fi
# You can set the `IMAGE_NAME` by export IMAGE_NAME="xxx".
if [ -z "${IMAGE_NAME}" ]; then
  IMAGE_NAME="dataomnis/${SERVICE_NAME}"
fi
# You can set the `IMAGE_TAG` by export IMAGE_TAG="xxx".
if [ -z "${IMAGE_TAG}" ]; then
  IMAGE_TAG=$(git --git-dir="${PROJECT_HOME}/.git" --work-tree="${PROJECT_HOME}" rev-parse --abbrev-ref HEAD | sed -e "s@^heads/@@")
fi

SERVICE_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

# check service image is exists.
if [ "$(docker image ls "${SERVICE_IMAGE}" | wc -l | awk '{print $1}')" -eq "1" ]; then
  echo "ERROR: SERVICE_IMAGE <${SERVICE_IMAGE}> not found, please build it first"
  exit 1
fi

IMAGE_URL="${CENTER_REPO}/${SERVICE_IMAGE}"

docker tag "${SERVICE_IMAGE}" "${IMAGE_URL}"
docker push "${IMAGE_URL}"
