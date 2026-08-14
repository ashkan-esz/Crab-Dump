# syntax=docker/dockerfile:1

ARG SING_BOX_VERSION=1.11.15

FROM golang:1.23-alpine AS singbox-builder

ARG SING_BOX_VERSION
ARG TARGETOS=linux
ARG TARGETARCH
ARG GOPROXY=https://proxy.golang.org,direct

RUN apk add --no-cache ca-certificates curl git tar
ENV GOPROXY="$GOPROXY"

WORKDIR /src

RUN curl -fsSL "https://github.com/SagerNet/sing-box/archive/refs/tags/v${SING_BOX_VERSION}.tar.gz" \
       | tar -xz --strip-components=1 \
    && mkdir -p /out \
    && case "$TARGETARCH" in \
         amd64|arm64) ;; \
         *) echo "unsupported sing-box architecture: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
       go build -trimpath \
         -ldflags "-X github.com/sagernet/sing-box/constant.Version=${SING_BOX_VERSION} -s -w -buildid=" \
         -tags "with_grpc,with_utls" \
         -o /out/sing-box ./cmd/sing-box \
    && test -x /out/sing-box

FROM rust:1-alpine AS builder

RUN apk add --no-cache build-base

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
COPY index.html users.html routing.html ./
# Build the final application into a separate target directory. This prevents
# Cargo from ever reusing the placeholder executable built for dependency
# caching above, even when Docker restores an otherwise valid stale layer.
RUN CARGO_TARGET_DIR=/build/final-target cargo build --release \
    && strip --strip-unneeded /build/final-target/release/crab-dump

FROM alpine:3.21

WORKDIR /app

RUN apk add --no-cache \
        ca-certificates \
        postgresql17-client \
    && mkdir -p /app/data /app/history /app/work \
    && chown -R postgres:postgres /app

COPY --from=builder --chown=postgres:postgres /build/final-target/release/crab-dump /app/crab-dump
COPY --from=singbox-builder /out/sing-box /usr/local/bin/sing-box

LABEL org.opencontainers.image.title="crab-dump" \
      org.opencontainers.image.description="Stream a compressed, optionally encrypted PostgreSQL dump to Telegram" \
      org.opencontainers.image.source="https://github.com/ashkan/crab-dump"

#USER postgres

ENTRYPOINT ["./crab-dump"]
