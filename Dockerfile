# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release \
    && rm -rf src target/release/deps/pg_backup_tg*

COPY src ./src
RUN cargo build --release

FROM postgres:17-bookworm

COPY --from=builder /build/target/release/pg-backup-tg /usr/local/bin/pg-backup-tg

USER postgres

LABEL org.opencontainers.image.title="pg-backup-tg" \
      org.opencontainers.image.description="Stream a compressed, optionally encrypted PostgreSQL dump to Telegram" \
      org.opencontainers.image.source="https://github.com/ashkan/pg-backup-tg"

ENTRYPOINT ["/usr/local/bin/pg-backup-tg"]
