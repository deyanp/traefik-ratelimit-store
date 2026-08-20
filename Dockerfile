# The runtime image carries no shell and no package manager, and the process runs as a
# non-root user that matches the securityContext in deploy/. There is nothing in here but
# the binary and the C runtime it links against.
#
# Both base images are pinned by digest, so a build today and a build next month compile
# the same toolchain onto the same runtime. The tag is kept beside the digest for the
# reader; the digest is what Docker uses. Bump both together, deliberately. Both digests
# are multi-architecture indexes, so the pins hold for every platform built below.
#
# The build stage always runs on the builder's own architecture and cross-compiles to the
# target. Emulating the toolchain instead would work, but this crate optimises with fat
# LTO in a single codegen unit, and that is the slowest possible thing to run under QEMU.

FROM --platform=$BUILDPLATFORM rust:1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS build

# Supplied by the builder, not by the caller: BUILDARCH is the machine doing the work,
# TARGETARCH the machine that will run the result. A plain `docker build` on an older
# engine sets neither, so both fall back to the architecture of the builder itself, which
# is what a local single-platform build wants anyway.
ARG BUILDARCH
ARG TARGETARCH

WORKDIR /src

# Cross-compiling needs a linker that emits the target's object format, and the target's
# own C runtime beside it — the cross gcc alone has no Scrt1.o or crti.o to link against,
# which fails only at the final link, long after everything has compiled. Building for the
# builder's own architecture needs neither, so that case installs nothing and uses the
# toolchain's default linker. The result is written to a file the later stages source,
# because each RUN is its own shell.
RUN set -eux; \
    native=$(dpkg --print-architecture); \
    targetarch="${TARGETARCH:-$native}"; \
    buildarch="${BUILDARCH:-$native}"; \
    case "$targetarch" in \
      amd64) target=x86_64-unknown-linux-gnu;  cross=gcc-x86-64-linux-gnu;  linker=x86_64-linux-gnu-gcc  ;; \
      arm64) target=aarch64-unknown-linux-gnu; cross=gcc-aarch64-linux-gnu; linker=aarch64-linux-gnu-gcc ;; \
      *) echo "unsupported TARGETARCH: $targetarch" >&2; exit 1 ;; \
    esac; \
    if [ "$targetarch" = "$buildarch" ]; then \
      linker=cc; \
    else \
      apt-get update; \
      apt-get install -y --no-install-recommends "$cross" "libc6-dev-${targetarch}-cross"; \
      rm -rf /var/lib/apt/lists/*; \
    fi; \
    rustup target add "$target"; \
    triple=$(echo "$target" | tr 'a-z' 'A-Z' | tr '-' '_'); \
    { echo "export CARGO_BUILD_TARGET=$target"; \
      echo "export CARGO_TARGET_${triple}_LINKER=$linker"; } > /env.sh; \
    cat /env.sh

# Dependencies first, so a source-only change does not rebuild the whole graph.
# The stub under tests/ exists only because the manifest declares a test target and cargo
# refuses to parse it otherwise; nothing here compiles tests.
COPY Cargo.toml Cargo.lock ./
RUN . /env.sh \
    && mkdir -p src tests \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && echo 'fn main() {}' > tests/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
# Touch so cargo does not mistake the real sources for the placeholder it just built.
# The binary is lifted to a fixed path because the target triple is in the output path and
# the COPY below cannot interpolate it.
RUN . /env.sh \
    && touch src/main.rs src/lib.rs \
    && cargo build --release --locked \
    && cp "target/$CARGO_BUILD_TARGET/release/traefik-ratelimit-store" /traefik-ratelimit-store

FROM gcr.io/distroless/cc-debian12@sha256:adcd20c7b4c988b73cbfbddb26d2eee574571e6d7c9ffea29b3821e0690efb77

COPY --from=build /traefik-ratelimit-store /usr/local/bin/traefik-ratelimit-store

# The protocol listener and the peer endpoint. Neither is privileged.
EXPOSE 6379 8080

USER nonroot
ENTRYPOINT ["/usr/local/bin/traefik-ratelimit-store"]
