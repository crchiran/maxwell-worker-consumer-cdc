FROM golang:1.24.3-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    go build -ldflags="-s -w" -o /worker .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata

RUN addgroup -g 10001 app && \
    adduser -D -u 10001 -G app app

USER app

COPY --from=builder /worker /worker

EXPOSE 8080

ENTRYPOINT ["/worker"]