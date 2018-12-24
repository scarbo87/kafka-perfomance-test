package sarama

import (
	"github.com/Shopify/sarama"
	"github.com/scarbo87/kafka-perfomance-test/types"
	"log"
)

type saramaSyncProducer struct {
	producer sarama.SyncProducer
	topic    string

	totalCount uint64
	errCount   uint64
}

func NewSaramaSyncProducer(cfg *types.SyncProducerConfig) *saramaSyncProducer {

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true

	config.Producer.RequiredAcks = sarama.RequiredAcks(cfg.Acks)

	switch cfg.Codec {
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	switch cfg.Acks {
	case -1:
	}

	producer, err := sarama.NewSyncProducer(cfg.BrokersAddress, config)
	if err != nil {
		log.Panicln(err)
	}

	return &saramaSyncProducer{
		producer: producer,
		topic:    cfg.Topic,
	}
}

func (p *saramaSyncProducer) Send(key, value []byte) {

	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(value),
		Key:   sarama.ByteEncoder(key),
	})
	if err != nil {
		p.errCount++
		log.Println(err)
	}
	p.totalCount++
}

func (p *saramaSyncProducer) GetTotalCount() uint64 {
	return p.totalCount
}

func (p *saramaSyncProducer) GetErrCount() uint64 {
	return p.errCount
}

func (p *saramaSyncProducer) Close() error {
	return p.producer.Close()
}
