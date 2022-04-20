ARG COMPILE_IMAGE
FROM ${COMPILE_IMAGE} as builder

ARG SERVICE
COPY ./${SERVICE}.yaml /opt/
COPY ./${SERVICE} /opt/

RUN upx /opt/${SERVICE}

###############################################################
FROM alpine:3.15

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk add -U tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

WORKDIR /opt/dataomnis
RUN mkdir -p /opt/dataomnis/conf; \
    mkdir -p /opt/dataomnis/bin;

ARG SERVICE
ENV SERVICE=${SERVICE}
COPY --from=builder /opt/grpc_health_probe /opt/dataomnis/bin
COPY --from=builder /opt/${SERVICE} /opt/dataomnis/bin
COPY --from=builder /opt/${SERVICE}.yaml /opt/dataomnis/conf

#CMD ["sh", "-c", "${SERVICE} start -c ${DATAOMNIS_CONF}/${SERVICE}.yaml" ]
CMD /opt/dataomnis/bin/${SERVICE} start -c /opt/dataomnis/conf/${SERVICE}.yaml
