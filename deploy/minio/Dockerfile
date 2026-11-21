FROM golang:1.25.1-alpine3.22 AS build
ARG MINIO_VERSION=RELEASE.2025-04-22T22-12-26Z
ADD https://github.com/minio/minio/archive/refs/tags/${MINIO_VERSION}.tar.gz /tmp/minio.tar.gz
WORKDIR /src
RUN tar -xzf /tmp/minio.tar.gz --strip-components=1
RUN GOMAXPROCS=2 CGO_ENABLED=0 go build -p 2 -trimpath -o /out/minio .

FROM alpine:3.22
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/minio /usr/local/bin/minio
RUN mkdir /data && chown 10001:10001 /data
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/minio"]
