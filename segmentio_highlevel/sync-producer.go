package segmentio_highlevel

import (
	"context"
	"github.com/scarbo87/kafka-perfomance-test/types"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/gzip"
	"github.com/segmentio/kafka-go/snappy"
	"log"
)

type segmentioSyncProducer struct {
	producer *kafka.Writer

	totalCount uint64
	errCount   uint64
}

func NewSegmentioSyncProducer(cfg *types.SyncProducerConfig) *segmentioSyncProducer {

	config := kafka.WriterConfig{
		Brokers:      cfg.BrokersAddress,
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: cfg.Acks,
		MaxAttempts:  1,
		BatchSize:    1,
	}

	switch cfg.Codec {
	case "gzip":
		config.CompressionCodec = gzip.NewCompressionCodec()
	case "snappy":
		config.CompressionCodec = snappy.NewCompressionCodec()
	default:
		config.CompressionCodec = nil
	}

	return &segmentioSyncProducer{
		producer: kafka.NewWriter(config),
	}
}

func (p *segmentioSyncProducer) Send(key, value []byte) {

	message := kafka.Message{Value: value, Key: key}
	err := p.producer.WriteMessages(context.Background(), message)
	if err != nil {
		p.errCount++
		log.Println(err)
	}
	p.totalCount++
}

func (p *segmentioSyncProducer) GetTotalCount() uint64 {
	return p.totalCount
}

func (p *segmentioSyncProducer) GetErrCount() uint64 {
	return p.errCount
}

func (p *segmentioSyncProducer) Close() error {
	return p.producer.Close()
}
