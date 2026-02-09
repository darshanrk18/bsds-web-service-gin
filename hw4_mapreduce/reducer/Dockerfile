FROM golang:1.25.6-alpine3.23 AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o reducer .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /app/reducer /app/reducer
EXPOSE 8080
CMD ["/app/reducer"]