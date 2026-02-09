FROM golang:1.25.6-alpine3.23 AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o reducer .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /app/reducer .
RUN chmod +x ./reducer
EXPOSE 8080
CMD ["./reducer"]