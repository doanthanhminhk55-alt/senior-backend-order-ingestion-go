# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/api \
    ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/producer \
    ./cmd/producer

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S app \
    && adduser -S app -G app

WORKDIR /app
COPY --from=build /out/api /app/api
COPY --from=build /out/producer /app/producer

USER app
EXPOSE 8080
CMD ["/app/api"]
