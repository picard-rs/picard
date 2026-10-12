FROM rust:1-bookworm AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --release -p living-city --features remoting,console \
    && cargo build --release -p picard-console

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /workspace/target/release/living-city /workspace/target/release/picard-console /usr/local/bin/
EXPOSE 8080 9100
ENTRYPOINT ["/usr/local/bin/living-city"]
