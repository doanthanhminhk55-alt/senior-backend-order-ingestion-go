# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build
WORKDIR /src

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/producer ./cmd/producer

FROM alpine:3.21 AS api
RUN addgroup -S app && adduser -S app -G app
COPY --from=build /out/api /usr/local/bin/api
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/api"]

FROM alpine:3.21 AS producer
RUN addgroup -S app && adduser -S app -G app
COPY --from=build /out/producer /usr/local/bin/producer
USER app
ENTRYPOINT ["/usr/local/bin/producer"]
