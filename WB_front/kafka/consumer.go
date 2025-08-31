package kafka

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// StartOrderAddConsumer обработчик json
func StartOrderConsumer(ctx context.Context, brokers []string, topic string, key string, groupID string, messageChan chan<- []byte) {
	go func() {
		for {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:        brokers,
				Topic:          topic,
				GroupID:        groupID,
				MaxWait:        1 * time.Second,
				CommitInterval: 1 * time.Second,
			})
			defer reader.Close()
			reader.SetOffsetAt(context.Background(), time.Now())

			log.Println("Starting order consumer...")
			for {
				select {
				case <-ctx.Done():
					return
				default:
					msg, err := reader.ReadMessage(ctx)
					if err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}
						log.Println("Error: %w", err)
						continue
					}
					if string(msg.Key) == key {
						messageChan <- msg.Value
						log.Printf("Processed %s: partition %d, offset %d",
							key, msg.Partition, msg.Offset)
					}
				}
			}
		}
	}()
}
