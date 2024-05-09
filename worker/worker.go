package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comments"
	// create a worker that's connected to the consumer
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("consumer started")
	// graceful shutdown
	sigchn := make(chan os.Signal, 1)
	signal.Notify(sigchn, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	donech := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf(
					"Received message count: %d | Topic (%s) | Message(%s)\n",
					msgCount,
					string(msg.Topic),
					string(msg.Value),
				)
			case <-sigchn:
				fmt.Println("Interruption detected")
				donech <- struct{}{}
			}
		}
	}()

	<-donech
	fmt.Println("Processed", msgCount, "messages")
	if err := worker.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
