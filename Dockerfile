# Multi-stage Dockerfile for evm-oracle-demo-oracle-service.
#
# Stage 1 (builder): installs pinned codegen toolchain (architecture rule 9),
# runs make proto-gen + go build with the same ldflags the Makefile uses.
#
# Stage 2 (runtime): distroless/nonroot. Reporter keys are NEVER baked into
# the image — they are mounted at runtime from /etc/lighthouse/secrets/
# (or a docker secret) per spec §3.2 and rule 5.

FROM golang:1.24-bookworm AS builder

WORKDIR /src

# Pinned codegen versions match the Makefile.
ARG BUF_VERSION=v1.55.0
ARG PROTOC_GEN_GO_VERSION=v1.36.0
ARG PROTOC_GEN_GO_GRPC_VERSION=v1.5.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git make \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates \
    && go install github.com/bufbuild/buf/cmd/buf@${BUF_VERSION} \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION} \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}

# Cache deps first.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN make build

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /src/bin/evm-oracle-demo-oracle-service /app/oracle
COPY --from=builder /src/db/migrations /app/migrations

USER nonroot:nonroot

# gRPC server (9090) + healthz/metrics (8080).
EXPOSE 9090 8080

ENTRYPOINT ["/app/oracle"]
CMD ["serve"]
