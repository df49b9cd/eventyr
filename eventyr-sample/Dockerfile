# syntax=docker/dockerfile:1
# Build the sample's service: stable Rust, the workspace's own target
# directory mounted in for the cached builds, the binary out the other
# side. The image is multi-stage so the runtime stays small.

FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# The recipe: from the workspace's lockfile and manifests. `cargo chef`
# pre-builds everything but the sample's own source, so the image's
# layer cache holds the dependencies.
FROM chef AS plan
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
RUN rustup toolchain install 1.99.0 --profile minimal
COPY --from=plan /app/recipe.json recipe.json
RUN cargo +1.99.0 chef cook --release --locked --recipe-path recipe.json -p eventyr-sample
COPY . .
RUN cargo +1.99.0 build --release --locked -p eventyr-sample

FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/eventyr-sample /usr/local/bin/eventyr-sample
EXPOSE 3000
ENTRYPOINT ["eventyr-sample"]
