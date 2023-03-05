FROM golang:latest

RUN mkdir /app
ADD . /app
WORKDIR /app
RUN go build -o ./main

CMD ["./main"]
# docker build -t cz4031_project1 .
# docker run cz4031_project1