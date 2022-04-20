#!/usr/bin/env bash

# exit now if shell command return not zero in this scripts;
set -e;

CURRENT_PATH=$(cd "$(dirname "${0}")" || exit 1; pwd)
cd "${CURRENT_PATH}"/..

usage(){
  echo "USAGE: $0 -s service"
  echo "OPTIONS:"
  echo "    -s required, the service that you wanted to compiled"
  echo "    -o, the output dir, default to ./build/bin/"
}

SERVICE=""
OUT_DIR="./build/bin"
# handle params
while getopts "hs:o:" opt; do
  case $opt in
    s)
      echo "SERVICE: ${OPTARG}"
      SERVICE="${OPTARG}"
      ;;
    o)
      echo "OUT_DIR: ${OPTARG}"
      OUT_DIR="${OPTARG}"
      ;;
    h) #help
      usage
      exit 0
      ;;
    ?)
      usage
      exit 1
  esac
done

# Check the parameters.
if [ -z "${SERVICE}" ]; then
  echo "The SERVICE name must be specified!"
  usage
  exit 1
fi
if [ -z "${OUT_DIR}" ]; then
  echo "The OUT_DIR must be specified!"
  usage
  exit 1
fi


#MODULE="$(go list -mod=mod)/cmds"
MODULE="github.com/DataWorkbench/common/utils/buildinfo"

ARGS=""
TAGS=""
if [ "${COMPILE_MODE}" == "release" ]; then
    TAGS="netgo jsoniter ${COMPILE_MODE}"
else
    TAGS="netgo jsoniter"
    ARGS="-race"
fi

go mod tidy
go mod download
go vet ./...
staticcheck ./...

mkdir -p "${OUT_DIR}"

go build ${ARGS} --tags "${TAGS}" -ldflags "
-X ${MODULE}.GoVersion=$(go version|awk '{print $3}')
-X ${MODULE}.CompileBy=$(git config user.email)
-X ${MODULE}.CompileTime=$(date '+%Y-%m-%d:%H:%M:%S')
-X ${MODULE}.GitBranch=$(git rev-parse --abbrev-ref HEAD)
-X ${MODULE}.GitCommit=$(git rev-parse --short HEAD)
-X ${MODULE}.OsArch=$(uname)/$(uname -m)
" \
-v -o "${OUT_DIR}/${SERVICE}" .
