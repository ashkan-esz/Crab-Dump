# syntax=docker/dockerfile:1

FROM alpine:3.21 AS singbox

ARG SING_BOX_VERSION=1.11.15
ARG TARGETARCH
RUN apk add --no-cache curl tar \
    && case "$TARGETARCH" in \
         amd64) arch=amd64 ;; \
         arm64) arch=arm64 ;; \
         *) echo "unsupported sing-box architecture: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && mkdir -p /out \
    && curl -fsSL "https://github.com/SagerNet/sing-box/releases/download/v${SING_BOX_VERSION}/sing-box-${SING_BOX_VERSION}-linux-${arch}.tar.gz" \
       | tar -xz --strip-components=1 -C /out \
    && test -x /out/sing-box

FROM rust:1-bookworm AS builder

WORKDIR /build

COPY Cargo.toml ./
RUN mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
COPY index.html users.html routing.html ./
# Build the final application into a separate target directory. This prevents
# Cargo from ever reusing the placeholder executable built for dependency
# caching above, even when Docker restores an otherwise valid stale layer.
RUN CARGO_TARGET_DIR=/build/final-target cargo build --release

FROM postgres:17-bookworm

WORKDIR /app

COPY --from=builder /build/final-target/release/crab-dump /app/crab-dump
COPY --from=singbox /out/sing-box /usr/local/bin/sing-box

RUN mkdir -p /app/data /app/history /app/work \
    && chown -R postgres:postgres /app

USER postgres

LABEL org.opencontainers.image.title="crab-dump" \
      org.opencontainers.image.description="Stream a compressed, optionally encrypted PostgreSQL dump to Telegram" \
      org.opencontainers.image.source="https://github.com/ashkan/crab-dump"

ENTRYPOINT ["./crab-dump"]
