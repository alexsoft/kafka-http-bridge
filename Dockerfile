FROM golang:1.26-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/kafka-http-bridge ./cmd/app

# Production stage
FROM scratch AS prod

LABEL org.opencontainers.image.source=https://github.com/alexsoft/kafka-http-bridge

WORKDIR /

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/bin/kafka-http-bridge /kafka-http-bridge

ENTRYPOINT ["/kafka-http-bridge"]
