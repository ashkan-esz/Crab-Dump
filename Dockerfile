# syntax=docker/dockerfile:1

ARG SING_BOX_VERSION=1.11.15
ARG SHOES_REVISION=7a5a8ee3bd1c52bc15ec57e074e95e374d41f275
ARG APP_VERSION=dev
ARG SING_BOX_IMAGE=scratch
ARG SHOES_IMAGE=scratch

FROM golang:1.23-alpine@sha256:383395b794dffa5b53012a212365d40c8e37109a626ca30d6151c8348d380b5f AS singbox-builder

ARG SING_BOX_VERSION
ARG TARGETOS=linux
ARG TARGETARCH
ARG GOPROXY=https://proxy.golang.org,direct
ARG GOAMD64=v1

RUN apk add --no-cache ca-certificates curl git tar
ENV GOPROXY="$GOPROXY"

WORKDIR /src

RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    curl -fsSL "https://github.com/SagerNet/sing-box/archive/refs/tags/v${SING_BOX_VERSION}.tar.gz" \
       | tar -xz --strip-components=1 \
    && mkdir -p /out \
    && case "$TARGETARCH" in \
         amd64|arm64) ;; \
         *) echo "unsupported sing-box architecture: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && if [ "$TARGETARCH" = "amd64" ]; then export GOAMD64="$GOAMD64"; fi \
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
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
COPY dashboard ./dashboard
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release --bin crab-dump \
    && strip --strip-unneeded /build/target/release/crab-dump

FROM rust:1-alpine AS shoes-builder

ARG SHOES_REVISION

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/root/.cargo/target,sharing=locked \
    apk add --no-cache build-base \
    && CARGO_TARGET_DIR=/root/.cargo/target cargo install --git https://github.com/cfal/shoes.git \
         --rev "$SHOES_REVISION" --locked \
    && test -x /usr/local/cargo/bin/shoes

FROM scratch AS singbox-image

COPY --from=singbox-builder /out/sing-box /out/sing-box

FROM scratch AS shoes-image

COPY --from=shoes-builder /usr/local/cargo/bin/shoes /usr/local/bin/shoes

FROM alpine:3.21 AS runtime-base

ARG APP_VERSION

WORKDIR /app

RUN apk add --no-cache \
        ca-certificates \
        postgresql17-client \
    && mkdir -p /app/data /app/history /app/work \
    && chown -R postgres:postgres /app

COPY --from=builder --chown=postgres:postgres /build/target/release/crab-dump /app/crab-dump
LABEL org.opencontainers.image.title="crab-dump" \
      org.opencontainers.image.description="Stream a compressed, optionally encrypted PostgreSQL dump to Telegram" \
      org.opencontainers.image.source="https://github.com/ashkan-esz/Crab-Dump" \
      org.opencontainers.image.version="${APP_VERSION}"

ENTRYPOINT ["./crab-dump"]

FROM runtime-base AS runtime-none

FROM runtime-base AS runtime-sing-box

COPY --from=singbox-builder /out/sing-box /usr/local/bin/sing-box

FROM runtime-base AS runtime-shoes

COPY --from=shoes-builder /usr/local/cargo/bin/shoes /usr/local/bin/shoes

FROM runtime-base AS runtime-all

COPY --from=singbox-builder /out/sing-box /usr/local/bin/sing-box
COPY --from=shoes-builder /usr/local/cargo/bin/shoes /usr/local/bin/shoes

FROM ${SING_BOX_IMAGE} AS prebuilt-singbox

FROM ${SHOES_IMAGE} AS prebuilt-shoes

FROM alpine:3.21 AS runtime-prebuilt

ARG APP_VERSION

WORKDIR /app

RUN apk add --no-cache \
        ca-certificates \
        postgresql17-client \
    && mkdir -p /app/data /app/history /app/work \
    && chown -R postgres:postgres /app

COPY --chown=postgres:postgres crab-dump /app/crab-dump
COPY --from=prebuilt-singbox /out/sing-box /usr/local/bin/sing-box
COPY --from=prebuilt-shoes /usr/local/bin/shoes /usr/local/bin/shoes

LABEL org.opencontainers.image.title="crab-dump" \
      org.opencontainers.image.description="Stream a compressed, optionally encrypted PostgreSQL dump to Telegram" \
      org.opencontainers.image.source="https://github.com/ashkan-esz/Crab-Dump" \
      org.opencontainers.image.version="${APP_VERSION}"

ENTRYPOINT ["./crab-dump"]

FROM runtime-prebuilt AS runtime-all-prebuilt

#HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
#    CMD ["sh", "-c", "wget -q -O /dev/null \"http://127.0.0.1:${API_PORT:-1111}/healthz\" || exit 1"]
