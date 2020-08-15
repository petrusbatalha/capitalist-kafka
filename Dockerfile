FROM ekidd/rust-musl-builder AS builder

WORKDIR /home/rust/

RUN USER=rust cargo new capitalist

WORKDIR /home/rust/capitalist

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

COPY src ./src
RUN LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu CFLAGS=-I/usr/local/musl/include CC=musl-gcc cargo build --release

FROM scratch
COPY --from=builder /home/rust/capitalist/target/x86_64-unknown-linux-musl/release/capitalist .
USER 1000
CMD ["./capitalist"]