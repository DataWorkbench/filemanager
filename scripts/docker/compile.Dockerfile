# Copyright 2020 The Dataomnis Authors. All rights reserved.
# Use of this source code is governed by a Apache license
# that can be found in the LICENSE file.

FROM golang:1.17.8-alpine3.15

ARG WORKSPACE=/opt
WORKDIR ${WORKSPACE}

# compress cmds (do not need to un-compress while run)
# Source In: https://github.com/upx/upx/releases/download/v3.95/upx-3.95-amd64_linux.tar.xz
RUN wget https://dataomnis-builder.gd2.qingstor.com/upx-3.95-amd64_linux.tar.xz && \
    tar -Jxf upx-3.95-amd64_linux.tar.xz && \
    mv upx-*/upx /usr/local/bin/ && \
    /bin/rm -fr upx-*

# install grpc_health_probe: v0.4.4 for status probe of dataomnis service on k8s
# Source In: https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.4/grpc_health_probe-linux-amd64
RUN wget -O grpc_health_probe https://dataomnis-builder.gd2.qingstor.com/grpc/grpc_health_probe-linux-amd64 && \
    chmod +x grpc_health_probe && \
    upx grpc_health_probe

# Source In; https://github.com/dominikh/go-tools/releases/download/2022.1/staticcheck_linux_amd64.tar.gz
RUN wget -nv https://dataomnis-builder.gd2.qingstor.com/go/staticcheck_linux_amd64.tar.gz && \
    tar zxf staticcheck_linux_amd64.tar.gz && \
    mv staticcheck/staticcheck /usr/local/bin/ && \
    /bin/rm -fr staticcheck*

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk add --no-cache bash git make openssl build-base
RUN apk add -U tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go env -w GOCACHE=/go/cache
