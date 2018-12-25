### Install on OSX
```
brew install librdkafka
brew install pkg-config
```

### Run test
```
go mod download
go build main.go
./main produce
```

### Performance numbers

The following performance numbers stem from tests using the following setup:

  * Intel Core i5 at 3.4GHz, 16GB of memory
  * One broker running on the same machine as kafka.
  * One topic with one partition.

#### First test

Configuration:
```
1) Test duration: 1m0s
5) Codec: snappy
6) Value size: 100 byte
7) Key size: 8 byte
8) Acks: -1

```

Result:

|           | total messages at 1 minute | messages/second |
|-----------|----------------------------|-----------------|
| sarama    | 428061                     | 7134            |
| segmentio | 609227                     | 10153           |
| confluent | 467549                     | 7792            |
