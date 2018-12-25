package segmentio

import (
	"context"
	"github.com/scarbo87/kafka-perfomance-test/types"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/gzip"
	"github.com/segmentio/kafka-go/snappy"
	"log"
)

type segmentioSyncProducer struct {
	conn             *kafka.Conn
	compressionCodec kafka.CompressionCodec

	totalCount uint64
	errCount   uint64
}

func NewSegmentioSyncProducer(cfg *types.SyncProducerConfig) *segmentioSyncProducer {

	conn, err := kafka.DialLeader(context.Background(), "tcp", cfg.BrokersAddress[0], cfg.Topic, 0)
	if err != nil {
		log.Panicln(err)
	}
	if err := conn.SetRequiredAcks(cfg.Acks); err != nil {
		log.Panicln(err)
	}

	p := &segmentioSyncProducer{
		conn: conn,
	}

	switch cfg.Codec {
	case "gzip":
		p.compressionCodec = gzip.NewCompressionCodec()
	case "snappy":
		p.compressionCodec = snappy.NewCompressionCodec()
	default:
		p.compressionCodec = nil
	}

	return p
}

func (p *segmentioSyncProducer) Send(key, value []byte) {

	message := kafka.Message{Value: value, Key: key}
	_, err := p.conn.WriteCompressedMessages(p.compressionCodec, message)
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
	return p.conn.Close()
}
