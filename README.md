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

  * Intel(R) Xeon(R) Platinum 8168 CPU @ 2.70GHz, 16GB of memory
  * One broker running on the same machine as kafka client.
  * One topic with one partition.

#### First test

Configuration go clients:
```
docker run --rm kafka-perfomance-test ./kafka-perfomance-test produce --brokers=127.0.0.1:9092 --client=segmentio --topic=segmentio --duration=60 --value-size=10

1) Test duration: 1m0s
2) Producer type: segmentio
3) Topic: segmentio
4) Codec: snappy
5) Value size: 10 byte
6) Key size: 8 byte
7) Acks: -1

```

Result:

|           | total mes at 60 sec | median mes/sec  |
|-----------|----------------------------|-----------------|
| sarama    | 378192                     | 6338            |
| segmentio | 526121                     | 8722            |
| confluent | 372564                     | 6170            |


for comparison java-client:
```
docker run --rm wurstmeister/kafka sh -c "/opt/kafka/bin/kafka-verifiable-producer.sh --topic java --max-messages 500000 --broker-list 127.0.0.1:9092 --value-prefix 1234567 --ack -1" > java.log

{"timestamp":1545776540601,"name":"startup_complete"}
{"timestamp":1545776540881,"name":"producer_send_success","key":null,"value":"1234567.0","partition":0,"offset":0,"topic":"java"}
-----
{"timestamp":1545776544293,"name":"producer_send_success","key":null,"value":"1234567.499999","partition":0,"offset":499999,"topic":"java"}
{"timestamp":1545776544305,"name":"shutdown_complete"}
{"timestamp":1545776544307,"name":"tool_data","sent":500000,"acked":500000,"target_throughput":-1,"avg_throughput":134952.76653171392}
```

**Sending 500K messages took just at 3.5 seconds !**
