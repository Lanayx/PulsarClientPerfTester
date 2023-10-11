package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://127.0.0.1:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-topic",
	})

	if err != nil {
		fmt.Println("Failed to publish message", err)
		return
	}

	payload := []byte("hello")
	ctx := context.Background()

	start := time.Now()
	n := 100000

	//warm up
	for i := 0; i < n/10; i++ {
		producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: payload,
		})
	}

	for i := 0; i < n; i++ {
		producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: payload,
		})
		if i%(n/100) == 0 {
			fmt.Println("Published message", i)
		}
	}

	elapsed := time.Since(start)
	speed := float64(n) / (elapsed.Seconds() * 1000)

	fmt.Printf("Loop executed in %s, speed: %.2fK iterations/second\n", elapsed, speed)
}
