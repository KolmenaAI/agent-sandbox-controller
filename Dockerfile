# Static musl build → minimal alpine. Alpine (not distroless) because the
# /execute endpoint needs `sh` (busybox); still ~10 MB total. The binary runs as
# a resident sidecar in every agent pod, so size + RSS matter.
FROM rust:1.94-alpine AS build

RUN apk add --no-cache musl-dev
WORKDIR /app

# Pre-compile dependencies for layer caching (stub main, then the real source).
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM alpine:3.24.1

RUN apk add --no-cache ca-certificates

# uid/gid 1001 matches the agent workspace volume ownership (fsGroup: 1001) so
# the controller can write to the mounted workspace.
RUN addgroup -g 1001 -S sandbox && adduser -S -u 1001 -G sandbox sandbox

COPY --from=build /app/target/release/agent-sandbox-controller /usr/local/bin/agent-sandbox-controller

USER sandbox
ENTRYPOINT ["agent-sandbox-controller"]
