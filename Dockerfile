FROM --platform=$BUILDPLATFORM golang:1.26-alpine@sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648 AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Cross-compile natively instead of emulating TARGETARCH: the builder always
# runs as BUILDPLATFORM, and GOOS/GOARCH steer the (CGO-free) Go toolchain.
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o bin/kafka-http-bridge ./cmd/app

# Production stage
FROM scratch AS prod

LABEL org.opencontainers.image.source=https://github.com/alexsoft/kafka-http-bridge

WORKDIR /

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/bin/kafka-http-bridge /kafka-http-bridge

USER 65532:65532

ENTRYPOINT ["/kafka-http-bridge"]
