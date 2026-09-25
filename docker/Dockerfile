FROM rust:1.86 AS arion-builder

RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/arion

COPY . .

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY ./arion-proxy/conf/arion-runtime.yaml /etc/arion/
COPY --chmod=+x ./docker/start_proxy.sh /start_proxy.sh
COPY --from=arion-builder /tmp/arion/target/release/arion /arion

ENTRYPOINT ["/start_proxy.sh"]
