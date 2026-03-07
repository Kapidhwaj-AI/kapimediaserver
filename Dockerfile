# syntax=docker/dockerfile:1.7

# -----------------------------
# Stage 1: Build Go binary
# -----------------------------
FROM golang:1.24-alpine AS builder

# Install dependencies
RUN apk add --no-cache git bash zip curl

# Set working directory
WORKDIR /app

# Copy the repo contents into the container
COPY . .

# Install dependencies
RUN go mod tidy

# Generate code and build static binary
RUN go generate ./... && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o mediamtx .

# -----------------------------
# Stage 2: Runtime container
# -----------------------------
FROM alpine:latest

RUN apk add --no-cache libc6-compat ca-certificates ffmpeg

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/mediamtx .

# Optionally copy config if it’s in your repo
#COPY mediamtx.yml /app/mediamtx.yml

# Expose typical ports
EXPOSE 8554/tcp 1935/tcp 8888/tcp 8889/tcp
EXPOSE 8000-8100/udp

# Default run command
CMD ["./mediamtx", "/app/mediamtx.yml"]
