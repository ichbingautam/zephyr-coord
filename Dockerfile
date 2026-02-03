# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /zephyr-coord ./cmd/zephyr-coord

# Runtime stage
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -S zephyr && adduser -S zephyr -G zephyr

# Copy binary
COPY --from=builder /zephyr-coord /usr/local/bin/

# Create data directory
RUN mkdir -p /data && chown zephyr:zephyr /data

# Switch to non-root user
USER zephyr

# Expose ports
EXPOSE 2181 2888 3888 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD echo "ruok" | nc localhost 2181 | grep -q "imok" || exit 1

# Volume for data persistence
VOLUME ["/data"]

# Entrypoint
ENTRYPOINT ["zephyr-coord"]
CMD ["-dataDir", "/data", "-listen", ":2181"]
