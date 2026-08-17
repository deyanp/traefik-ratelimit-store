# The runtime image carries no shell and no package manager, and the process runs as a
# non-root user that matches the securityContext in deploy/. There is nothing in here but
# the binary and the C runtime it links against.

FROM rust:1-bookworm AS build

WORKDIR /src

# Dependencies first, so a source-only change does not rebuild the whole graph.
# The stub under tests/ exists only because the manifest declares a test target and cargo
# refuses to parse it otherwise; nothing here compiles tests.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src tests \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && echo 'fn main() {}' > tests/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
# Touch so cargo does not mistake the real sources for the placeholder it just built.
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /src/target/release/traefik-ratelimit-store /usr/local/bin/traefik-ratelimit-store

# The protocol listener and the peer endpoint. Neither is privileged.
EXPOSE 6379 8080

USER nonroot
ENTRYPOINT ["/usr/local/bin/traefik-ratelimit-store"]
