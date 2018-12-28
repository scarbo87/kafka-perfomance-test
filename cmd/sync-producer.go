package cmd

import (
	"context"
	"encoding/binary"
	"github.com/montanaflynn/stats"
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
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		producerConfig.Duration = time.Second * time.Duration(producerConfig.DurationSecond)
	},
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

		// graceful shutdown & context
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)

		ctx, cancel := context.WithTimeout(context.Background(), producerConfig.Duration)
		defer cancel()

		// stat ticker
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		metrics := make([]float64, 0, 1000)
		go func() {
			old := producer.GetTotalCount()
			for range ticker.C {
				metrics = append(metrics, float64(producer.GetTotalCount()-old))
				old = producer.GetTotalCount()
			}
		}()

		// start test
		log.Println("Start test")
		var i uint64
		var value = make([]byte, producerConfig.ValueSize)
		start := time.Now()

		reportFunc := func() {
			median, _ := stats.Percentile(metrics, 50)
			percent1, _ := stats.PercentileNearestRank(metrics, 1)
			percent99, _ := stats.PercentileNearestRank(metrics, 99)

			log.Printf(`End test
Report:
1) Test time: %s
2) Total messages sent: %d
3) Of them with an error: %d
4) Median mes/sec: %f
5) The worse (1 percentile) mes/sec: %f
6) The best (99 percentile) mes/sec: %f
`,
				time.Now().Sub(start),
				producer.GetTotalCount(),
				producer.GetErrCount(),
				median,
				percent1,
				percent99,
			)
		}

		for {
			select {
			case <-ctx.Done():
				reportFunc()
				return
			case <-signalChan:
				signal.Stop(signalChan)
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

	syncProducerCmd.Flags().StringVarP(&producerConfig.ProducerType, "client", "c", "segmentio", "Available clients type: sarama|segmentio|segmentio-highlevel|confluent")
	syncProducerCmd.Flags().StringVarP(&producerConfig.Topic, "topic", "t", "example", "")
	syncProducerCmd.Flags().StringVarP(&producerConfig.Codec, "codec", "", "snappy", "Enable compression: none|gzip|snappy")
	syncProducerCmd.Flags().UintVarP(&producerConfig.DurationSecond, "duration", "d", 60, "Test time duration of sec")
	syncProducerCmd.Flags().UintVarP(&producerConfig.ValueSize, "value-size", "", 100, "Message size in bytes")
	syncProducerCmd.Flags().UintVarP(&producerConfig.KeySize, "key-size", "", 8, "Key size in bytes")
	syncProducerCmd.Flags().IntVarP(&producerConfig.Acks, "acks", "a", -1, "Required Acks: -1, 0, 1")
	syncProducerCmd.Flags().StringArrayVarP(&producerConfig.BrokersAddress, "brokers", "b", []string{":9092"}, "Brokers address: 127.0.0.1:9092")
}
