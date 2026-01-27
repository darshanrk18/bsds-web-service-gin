# syntax=docker/dockerfile:1

FROM golang:1.25.5

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

EXPOSE 8080

CMD ["/docker-gs-ping"]