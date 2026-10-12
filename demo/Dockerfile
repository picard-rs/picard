FROM rust:1-bookworm AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --release -p living-city --features remoting

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /workspace/target/release/living-city /usr/local/bin/living-city
EXPOSE 8080 9100
ENTRYPOINT ["/usr/local/bin/living-city"]
