FROM rustlang/rust:nightly

RUN --mount=type=secret,id=host_ca \
    if [ -f /run/secrets/host_ca ]; then \
      cat /etc/ssl/certs/ca-certificates.crt /run/secrets/host_ca > /tmp/ca-bundle.pem; \
      CARGO_HTTP_CAINFO=/tmp/ca-bundle.pem cargo install cargo-fuzz --locked; \
    else \
      cargo install cargo-fuzz --locked; \
    fi

WORKDIR /work
