# The runtime image carries no shell and no package manager, and the process runs as a
# non-root user that matches the securityContext in deploy/. There is nothing in here but
# the binary and the C runtime it links against.
#
# Both base images are pinned by digest, so a build today and a build next month compile
# the same toolchain onto the same runtime. The tag is kept beside the digest for the
# reader; the digest is what Docker uses. Bump both together, deliberately.

FROM rust:1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS build

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

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:adcd20c7b4c988b73cbfbddb26d2eee574571e6d7c9ffea29b3821e0690efb77

COPY --from=build /src/target/release/traefik-ratelimit-store /usr/local/bin/traefik-ratelimit-store

# The protocol listener and the peer endpoint. Neither is privileged.
EXPOSE 6379 8080

USER nonroot
ENTRYPOINT ["/usr/local/bin/traefik-ratelimit-store"]
