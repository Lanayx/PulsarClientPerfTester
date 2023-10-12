package main

import (
	"context"
	"fmt"
	"log"
	"sync"
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

	n := 1000000

	wg := &sync.WaitGroup{}

	//warm up
	for j := 1; j <= n/10000; j++ {
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			producer.SendAsync(ctx, &pulsar.ProducerMessage{
				Payload: payload,
			}, func(mid pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
				defer wg.Done()
			})
		}
		wg.Wait()
	}

	start := time.Now()
	for j := 1; j <= n/1000; j++ {
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			producer.SendAsync(ctx, &pulsar.ProducerMessage{
				Payload: payload,
			}, func(mid pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
				defer wg.Done()
			})
		}
		wg.Wait()
		if j%10 == 0 {
			fmt.Println("Published message", j*1000)
		}
	}

	elapsed := time.Since(start)
	speed := float64(n) / (elapsed.Seconds() * 1000)

	fmt.Printf("Loop executed in %s, speed: %.2fK iterations/second\n", elapsed, speed)
}
