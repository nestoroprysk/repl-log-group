FROM golang:1.17.1

WORKDIR /app

COPY . .

RUN go mod download

CMD [ "go", "test", "/app", "-count=1" ]
