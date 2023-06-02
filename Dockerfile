FROM gcr.io/distroless/cc
COPY target/release/my-proxy /
ENTRYPOINT ["/my-proxy"]
