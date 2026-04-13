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

ENTRYPOINT ["/usr/local/bin/binnacle"]
CMD ["--data-dir=/data", "--log-level=info"]
