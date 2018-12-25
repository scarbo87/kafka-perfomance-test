FROM golang:latest

RUN git clone https://github.com/edenhill/librdkafka.git; cd librdkafka; ./configure --prefix /usr; make; make install

RUN mkdir -p /go/src/github.com/scarbo87/kafka-perfomance-test
WORKDIR /go/src/github.com/scarbo87/kafka-perfomance-test

ADD ./ /go/src/github.com/scarbo87/kafka-perfomance-test

RUN GO111MODULE=on go mod download
RUN GO111MODULE=on go build -o kafka-perfomance-test main.go
