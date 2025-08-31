FROM golang:1.24-alpine

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -v -o /app/bin/front-app ./main.go

CMD ["/app/bin/front-app"]