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

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "my-topic",
		SubscriptionName: "my-sub",
	})

	if err != nil {
		fmt.Println("Failed to subscribe", err)
		return
	}

	ctx := context.Background()
	n := 10000000
	//warmup
	for i := 0; i < n/10; i++ {
		message, _ := consumer.Receive(ctx)
		consumer.AckID(message.ID())
	}

	start := time.Now()

	for i := 0; i < n; i++ {
		message, _ := consumer.Receive(ctx)
		consumer.AckID(message.ID())
		if i%(n/100) == 0 {
			fmt.Println("Received message", i)
		}
	}

	elapsed := time.Since(start)
	speed := float64(n) / (elapsed.Seconds() * 1000)

	fmt.Printf("Loop executed in %s, speed: %.2fK iterations/second\n", elapsed, speed)
}
