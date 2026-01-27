# syntax=docker/dockerfile:1

FROM golang:1.25.6-alpine3.23

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o server main.go

EXPOSE 8080

CMD ["./server"]