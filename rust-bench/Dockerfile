FROM rust:1.98.1-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build

# Dependency layer: build librdkafka and all crates against a stub main so
# source edits do not rebuild them.
COPY Cargo.toml Cargo.lock build.rs ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
RUN touch src/main.rs \
    && cargo test --release --locked \
    && cargo build --release --locked \
    && ldd target/release/kbench-rust

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/kbench-rust /usr/local/bin/kbench-rust
ENTRYPOINT ["/usr/local/bin/kbench-rust"]
