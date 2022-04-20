# Copyright (C) 2020 Yunify, Inc.

VERBOSE = no

.PHONY: help
help:
	@echo "Please use \`make <target>\` where <target> is one of"
	@echo "  format   to format the code"
	@echo "  vet      to run golang vet"
	@echo "  lint     to run the staticcheck"
	@echo "  check    to format, vet, lint"
	@echo "  test     to run test case"
	@echo "  compile  compile binary program with debug mode"
	@echo "  release  compile binary program with release mode"
	@echo "  run      run server for test"
	@exit 0

COMPILE = _compile() {    \
    args="$(filter-out $@,$(MAKECMDGOALS))"; \
    if [ ${VERBOSE} = "yes" ]; then        \
        bash -x ./scripts/compile_in_local.sh $$args;   \
    else                                     \
        bash ./scripts/compile_in_local.sh $$args;      \
    fi                                       \
}

.PHONY: format
format:
	@[ ${VERBOSE} = "yes" ] && set -x; go fmt ./...;

.PHONY: vet
vet:
	@[ ${VERBOSE} = "yes" ] && set -x; go vet ./...;

.PHONY: lint
lint:
	@[ ${VERBOSE} = "yes" ] && set -x; staticcheck ./...;

.PHONY: tidy
tidy:
	@[ ${VERBOSE} = "yes" ] && set -x; go mod tidy;

.PHONY: check
check: tidy format vet lint

.PHONY: test
test:
	@[ ${VERBOSE} = "yes" ] && set -x; go test -race -v ./... -test.count=1 -failfast

.PHONY: compile
compile:
	@$(COMPILE); _compile

.PHONY: release
release:
	@$(COMPILE); export COMPILE_MODE="release"; _compile

.PHONY: run
run:
	@[ ${VERBOSE} = "yes" ] && sh -x ./scripts/run.sh || sh ./scripts/run.sh

.PHONY: build
build: compile-image build-image push-image

.PHONY: compile-image
compile-image:
	@[ ${VERBOSE} = "yes" ] && sh -x ./scripts/compile_in_docker.sh || sh ./scripts/compile_in_docker.sh

.PHONY: build-image
build-image:
	@[ ${VERBOSE} = "yes" ] && sh -x ./scripts/build_image.sh || sh ./scripts/build_image.sh

.PHONY: push-image
push-image:
	@[ ${VERBOSE} = "yes" ] && sh -x ./scripts/push_image.sh || sh ./scripts/push_image.sh


.DEFAULT_GOAL = help

# Target name % means that it is a rule that matches anything, @: is a recipe;
# the : means do nothing
%:
	@:
