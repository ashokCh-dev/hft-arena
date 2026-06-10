# Sandbox template: Rust submission.
# The orchestrator drops the contestant's source in as main.rs before building
# (placed at src/main.rs for Cargo).
FROM rust:1.81 AS build
WORKDIR /app
COPY Cargo.toml .
COPY main.rs src/main.rs
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* && useradd -m runner
COPY --from=build /app/target/release/engine /usr/local/bin/engine
USER runner
EXPOSE 9000
CMD ["engine"]
