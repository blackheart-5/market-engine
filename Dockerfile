# Build the Go binaries
FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/producer ./cmd/producer && \
    CGO_ENABLED=0 go build -o /out/consumer ./cmd/consumer

# Runtime: Redpanda image + our binaries, all in one machine
FROM docker.redpanda.com/redpandadata/redpanda:v24.2.7
WORKDIR /app
COPY --from=build /out/producer /out/consumer /app/
COPY web /app/web
COPY --chmod=755 deploy/start.sh /app/start.sh
ENV KAFKA_BROKERS=localhost:9092 PORT=8090
EXPOSE 8090
ENTRYPOINT ["/app/start.sh"]
