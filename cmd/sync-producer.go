package cmd

import (
	"context"
	"encoding/binary"
	"github.com/scarbo87/kafka-perfomance-test/confluent"
	"github.com/scarbo87/kafka-perfomance-test/sarama"
	"github.com/scarbo87/kafka-perfomance-test/segmentio"
	"github.com/scarbo87/kafka-perfomance-test/segmentio_highlevel"
	"github.com/scarbo87/kafka-perfomance-test/types"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var producerConfig = &types.SyncProducerConfig{}

// syncProducerCmd represents the produce command
var syncProducerCmd = &cobra.Command{
	Use:   "produce",
	Short: "publish messages to kafka",
	Run: func(cmd *cobra.Command, args []string) {

		log.Printf(`Run test with configuration:
1) Test duration: %s
2) Producer type: %s
4) Topic: %s
5) Codec: %s
6) Value size: %d byte
7) Key size: %d byte
8) Acks: %d
`,
			producerConfig.Duration,
			producerConfig.ProducerType,
			producerConfig.Topic,
			producerConfig.Codec,
			producerConfig.ValueSize,
			producerConfig.KeySize,
			producerConfig.Acks,
		)

		var producer types.Producer
		switch producerConfig.ProducerType {
		case "sarama":
			producer = sarama.NewSaramaSyncProducer(producerConfig)
		case "segmentio":
			producer = segmentio.NewSegmentioSyncProducer(producerConfig)
		case "segmentio-writer":
			producer = segmentio_highlevel.NewSegmentioSyncProducer(producerConfig)
		case "confluent":
			producer = confluent.NewConfluentProducer(producerConfig)
		default:
			log.Panicln("undefined producer type")
		}
		defer producer.Close()

		// one test publish
		producer.Send([]byte("ping"), []byte("ping"))

		// graceful shutdown
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)

		// test context
		ctx, cancel := context.WithTimeout(context.Background(), producerConfig.Duration)
		defer cancel()

		reportFunc := func() {
			log.Printf(`End test
Report:
1) Total messages sent: %d
2) Of them with an error: %d
`,
				producer.GetTotalCount(),
				producer.GetErrCount(),
			)
		}

		log.Println("Start test")
		var i uint64
		var value = make([]byte, producerConfig.ValueSize)
		for {
			select {
			case <-ctx.Done():
				reportFunc()
				return
			case <-signalChan:
				reportFunc()
				return
			default:
				key := make([]byte, producerConfig.KeySize)
				binary.LittleEndian.PutUint64(key, i)
				producer.Send(key, value)
				i++
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(syncProducerCmd)

	syncProducerCmd.Flags().StringVarP(&producerConfig.ProducerType, "client", "c", "sarama", "Available clients type: sarama|segmentio|segmentio-writer|confluent")
	syncProducerCmd.Flags().StringVarP(&producerConfig.Topic, "topic", "t", "example", "")
	syncProducerCmd.Flags().StringVarP(&producerConfig.Codec, "codec", "z", "snappy", "Enable compression: none|gzip|snappy")
	syncProducerCmd.Flags().UintVarP(&producerConfig.ValueSize, "value-size", "v", 100, "Message size in bytes")
	syncProducerCmd.Flags().UintVarP(&producerConfig.KeySize, "key-size", "k", 8, "Key size in bytes")
	syncProducerCmd.Flags().IntVarP(&producerConfig.Acks, "acks", "a", -1, "Required Acks: -1, 0, 1")
	syncProducerCmd.Flags().StringArrayVarP(&producerConfig.BrokersAddress, "brokers", "b", []string{":9092"}, "")

	var duration uint
	syncProducerCmd.Flags().UintVarP(&duration, "duration", "d", 60, "Test time duration of sec")
	producerConfig.Duration = time.Second * time.Duration(duration)
}
