package types

import "time"

type Producer interface {
	Send(key, value []byte)
	GetTotalCount() uint64
	GetErrCount() uint64
	Close() error
}

type SyncProducerConfig struct {
	Duration       time.Duration
	DurationSecond uint
	BrokersAddress []string
	ProducerType   string
	Topic          string
	ValueSize      uint
	KeySize        uint
	Acks           int
	Codec          string
}
