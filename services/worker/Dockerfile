# Build stage. go.mod and go.sum are copied first so the dependency layer is
# reused whenever only application code changes.
FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# -trimpath keeps absolute build paths out of the binary; static linking lets the
# result run in a distroless-style image with no libc.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget \
    && adduser -D -u 10001 worker
COPY --from=build /out/worker /usr/local/bin/worker

# The worker holds a consumer-group identity and processes business events; it
# has no reason to run as root.
USER 10001
EXPOSE 9100

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://localhost:9100/readyz >/dev/null || exit 1

ENTRYPOINT ["worker"]
