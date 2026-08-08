# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
RUN cargo build --release

FROM postgres:17-bookworm

COPY --from=builder /build/target/release/crab-dump /usr/local/bin/crab-dump

USER postgres

LABEL org.opencontainers.image.title="crab-dump" \
      org.opencontainers.image.description="Stream a compressed, optionally encrypted PostgreSQL dump to Telegram" \
      org.opencontainers.image.source="https://github.com/ashkan/crab-dump"

ENTRYPOINT ["/usr/local/bin/crab-dump"]
