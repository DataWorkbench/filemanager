#!/usr/bin/env bash

current_path=$(cd "$(dirname "${0}")" || exit 1; pwd)
cd "${current_path}"/.. || exit 1

# export $(xargs < config/config.env)

while read -r line; do
    if grep -v "^$" <<< "$line"| grep -v "^#" >/dev/null; then

      export "${line//\"/}"
    fi
done <"config/config.env"

go run -race main.go -c config/config.yaml start

exit 0
