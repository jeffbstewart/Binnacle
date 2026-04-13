# syntax=docker/dockerfile:1.7

# ---------- build stage ----------
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Cache module downloads. go.sum is optional in a zero-deps module.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/binnacle \
      .

# ---------- runtime stage ----------
# distroless/static: smallest possible runtime with CA certs and tzdata,
# runs as an unprivileged user by default.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/binnacle /usr/local/bin/binnacle

EXPOSE 4317 4318 8088

# The binary can health-check itself — saves bundling curl or wget into
# the distroless image. `binnacle --healthcheck` GETs /api/logs/health on
# the query port and exits 0/1. Both are in exec form because distroless
# has no shell.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/binnacle", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/binnacle"]
CMD ["--data-dir=/data", "--log-level=info"]
