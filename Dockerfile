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

# Seed an empty /data directory that the runtime stage will COPY with
# an explicit ownership (1046:100). When Docker mounts an empty named
# volume at /data on first use, the volume inherits ownership from
# this mount point — so the process UID defined below can actually
# write the SQLite database. Without this step the volume is root-
# owned and the process exits with SQLITE_CANTOPEN.
RUN mkdir -p /out/data

# ---------- runtime stage ----------
# distroless/static: smallest possible runtime with CA certs and tzdata.
# We don't use the :nonroot tag because we need a Synology-matching
# UID (1046:100), which we set explicitly via USER below — UIDs do not
# need to exist in /etc/passwd for Linux to honor them.
FROM gcr.io/distroless/static-debian12

COPY --from=builder /out/binnacle /usr/local/bin/binnacle
COPY --from=builder --chown=1046:100 /out/data /data

# 1046:100 matches the `app:users` user MediaManager creates in its
# own image — keeping the two services aligned means the same Synology
# user owns logs from both writers when volumes are bind-mounted.
USER 1046:100

EXPOSE 4317 4318 8088

# The binary can health-check itself — saves bundling curl or wget into
# the distroless image. `binnacle --healthcheck` GETs /api/logs/health on
# the query port and exits 0/1. Both are in exec form because distroless
# has no shell.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/binnacle", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/binnacle"]
CMD ["--data-dir=/data", "--log-level=info"]
