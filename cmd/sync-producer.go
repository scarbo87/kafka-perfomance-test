package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/scarbo87/kafka-perfomance-test/confluent"
	"github.com/scarbo87/kafka-perfomance-test/sarama"
	"github.com/scarbo87/kafka-perfomance-test/segmentio"
	"github.com/scarbo87/kafka-perfomance-test/segmentio_highlevel"
	"github.com/scarbo87/kafka-perfomance-test/types"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/signal"
	"sync"
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
9) Number producers: %d
10) Count partitions: %d
`,
			producerConfig.Duration,
			producerConfig.ProducerType,
			producerConfig.Topic,
			producerConfig.Codec,
			producerConfig.ValueSize,
			producerConfig.KeySize,
			producerConfig.Acks,
			producerConfig.NumberProducers,
			producerConfig.CountPartitions,
		)

		wg := sync.WaitGroup{}
		wg.Add(int(producerConfig.NumberProducers))
		producers := make([]types.Producer, 0, producerConfig.NumberProducers)
		k := 0
		for j := 0; j < int(producerConfig.NumberProducers); j++ {

			var producer types.Producer
			switch producerConfig.ProducerType {
			case "sarama":
				producer = sarama.NewSaramaSyncProducer(producerConfig)
			case "segmentio":
				producer = segmentio.NewSegmentioSyncProducer(producerConfig, k)
			case "segmentio-writer":
				producer = segmentio_highlevel.NewSegmentioSyncProducer(producerConfig)
			case "confluent":
				producer = confluent.NewConfluentProducer(producerConfig)
			default:
				log.Panicln("undefined producer type")
			}
			defer producer.Close()

			if k >= int(producerConfig.CountPartitions)-1 {
				k = 0
			}
			k++

			producers = append(producers, producer)
			go func() {
				defer wg.Done()
				// one test publish
				producer.Send([]byte("ping"), []byte("ping"))
			}()
		}
		wg.Wait()

		// graceful shutdown & context
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)

		ctx, cancel := context.WithTimeout(context.Background(), producerConfig.Duration)
		defer cancel()

		// stat ticker
		//ticker := time.NewTicker(time.Second * 1)
		//defer ticker.Stop()
		//metrics := make([]float64, 0, 1000)
		//
		//wg.Add(int(producerConfig.NumberProducers))
		//for j := range producers {
		//	go func(producer types.Producer) {
		//		defer wg.Done()
		//
		//		old := producer.GetTotalCount()
		//		for range ticker.C {
		//			metrics = append(metrics, float64(producer.GetTotalCount()-old))
		//			old = producer.GetTotalCount()
		//		}
		//	}(producers[j])
		//}
		//wg.Wait()

		// start test
		log.Println("Start test")
		start := time.Now()

		go func() {
			<-signalChan
			signal.Stop(signalChan)
			cancel()
		}()

		var value = make([]byte, producerConfig.ValueSize)
		wg.Add(int(producerConfig.NumberProducers))
		for j := range producers {
			go func(ctx context.Context, producer types.Producer, pn int) {
				defer wg.Done()

				var i uint64
				for {
					select {
					case <-ctx.Done():
						fmt.Printf("Producer #%d stopped\n", pn)
						return
					default:
						key := make([]byte, producerConfig.KeySize)
						binary.LittleEndian.PutUint64(key, i)
						producer.Send(key, value)
						i++
					}
				}
			}(ctx, producers[j], j)
		}
		wg.Wait()

		var totalCount uint64
		var totalErrors uint64
		for j := range producers {
			totalCount += producers[j].GetTotalCount()
			totalErrors += producers[j].GetErrCount()
		}

		log.Printf(`End test
Report:
1) Test time: %s
2) Total messages sent: %d
3) Of them with an error: %d
`,
			time.Now().Sub(start),
			totalCount,
			totalErrors,
		)
	},
}

func init() {
	rootCmd.AddCommand(syncProducerCmd)

	syncProducerCmd.Flags().StringVarP(&producerConfig.ProducerType, "client", "c", "sarama", "Available clients type: sarama|segmentio|segmentio-highlevel|confluent")
	syncProducerCmd.Flags().StringVarP(&producerConfig.Topic, "topic", "t", "example", "")
	syncProducerCmd.Flags().StringVarP(&producerConfig.Codec, "codec", "", "snappy", "Enable compression: none|gzip|snappy")
	syncProducerCmd.Flags().UintVarP(&producerConfig.DurationSecond, "duration", "d", 60, "Test time duration of sec")
	syncProducerCmd.Flags().UintVarP(&producerConfig.ValueSize, "value-size", "", 100, "Message size in bytes")
	syncProducerCmd.Flags().UintVarP(&producerConfig.KeySize, "key-size", "", 8, "Key size in bytes")
	syncProducerCmd.Flags().UintVarP(&producerConfig.NumberProducers, "number-producers", "", 1, "Number of concurrent producers")
	syncProducerCmd.Flags().UintVarP(&producerConfig.CountPartitions, "count-partitions", "", 1, "Count partitions of topic")
	syncProducerCmd.Flags().IntVarP(&producerConfig.Acks, "acks", "a", -1, "Required Acks: -1, 0, 1")
	syncProducerCmd.Flags().StringArrayVarP(&producerConfig.BrokersAddress, "brokers", "b", []string{":9092"}, "Brokers address: 127.0.0.1:9092")
}
