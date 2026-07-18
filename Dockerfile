# syntax=docker/dockerfile:1
# ---- build stage ----
FROM rust:1-bookworm AS builder

WORKDIR /build
# Cache deps: copy manifests first, fetch, then copy source.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src target/release/deps/pg_backup_tg*

COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---- runtime stage ----
# postgres:<pgmajor>-bookworm gives us `pg_dump` matching major 17,
# pinned to the same Debian baseline as the builder (bookworm = glibc compat).
FROM postgres:17-bookworm

# The binary, statically linked against bundled C deps except libc.
COPY --from=builder /build/target/release/pg-backup-tg /usr/local/bin/pg-backup-tg

# Non-root user (the postgres image provides the `postgres` user).
USER postgres

LABEL org.opencontainers.image.title="pg-backup-tg" \
      org.opencontainers.image.description="Stream an encrypted, compressed PostgreSQL dump to Telegram" \
      org.opencontainers.image.source="https://github.com/ashkan/pg-backup-tg"

ENTRYPOINT ["/usr/local/bin/pg-backup-tg"]
