package confluent

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/scarbo87/kafka-perfomance-test/types"
	"log"
)

type confluentProducer struct {
	producer *kafka.Producer
	topic    string

	totalCount uint64
	errCount   uint64
}

func NewConfluentProducer(cfg *types.SyncProducerConfig) *confluentProducer {

	config := kafka.ConfigMap{
		"bootstrap.servers":       cfg.BrokersAddress[0],
		"broker.version.fallback": "2.1.0.0",
		"session.timeout.ms":      6000,
		"produce.offset.report":   true,
		//"queue.buffering.max.messages": 1,
		"default.topic.config": kafka.ConfigMap{"acks": cfg.Acks},
		//
		"go.delivery.reports": true,
		//"go.batch.producer":   true,
	}

	p, err := kafka.NewProducer(&config)
	if err != nil {
		log.Panicln(err)
	}

	return &confluentProducer{
		producer: p,
		topic:    cfg.Topic,
	}
}

func (p *confluentProducer) Send(key, value []byte) {

	deliveryChan := make(chan kafka.Event)
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          value,
		Key:            key,
	}, deliveryChan)
	if err != nil {
		p.errCount++
		p.totalCount++
		log.Println(err)
		close(deliveryChan)
		return
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)
	close(deliveryChan)

	if m.TopicPartition.Error != nil {
		p.errCount++
		log.Println(err)
	}
	p.totalCount++
}

func (p *confluentProducer) GetTotalCount() uint64 {
	return p.totalCount
}

func (p *confluentProducer) GetErrCount() uint64 {
	return p.errCount
}

func (p *confluentProducer) Close() error {
	p.producer.Close()
	return nil
}
