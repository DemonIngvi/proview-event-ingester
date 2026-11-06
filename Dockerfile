# syntax=docker/dockerfile:1.7
# Build stage: static binary, reproducible, cached module downloads.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/event-ingester ./cmd/event-ingester

# Runtime stage: distroless, non-root, no shell.
FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/DemonIngvi/proview-event-ingester" \
      org.opencontainers.image.description="ProView event-ingester: Kafka to ClickHouse batch loader" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/event-ingester /usr/local/bin/event-ingester
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/event-ingester"]
