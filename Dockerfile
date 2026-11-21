FROM golang:1.25.1-alpine3.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=movie-service
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -p 2 -trimpath -o /out/service ./cmd/${SERVICE}

FROM build AS integration-test
CMD ["go", "test", "-count=1", "-v", "./tests/integration"]

FROM alpine:3.22 AS runtime
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/service /usr/local/bin/service
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/service"]
