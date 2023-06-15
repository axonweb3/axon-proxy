FROM gcr.io/distroless/cc
COPY target/release/axon-proxy /
ENTRYPOINT ["/axon-proxy"]
