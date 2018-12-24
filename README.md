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

