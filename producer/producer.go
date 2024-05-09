package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v3"
)

// Comment Structure
type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	// create new app (sort of like ExpressJS)
	app := fiber.New()
	// create api group
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	// server running on port 3000
	app.Listen(":3000")
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	// define where broker (kafka) lives
	brokersUrl := []string{"localhost:29092"}
	// connect to producer
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()

	// format the message (topic, value)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	// send message via the producer
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	// on success, print message
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func createComment(c fiber.Ctx) error {
	cmt := new(Comment)

	// bind the response body to the comment struct
	if err := c.Bind().Body(cmt); err != nil {
		// if error, return 400
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}

	// marshal comment struct into byte[] for kafka push
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		fmt.Printf("cannot marshal comment into bytes", err)
		return err
	}

	// push message to kafka
	err = PushCommentToQueue("comments", cmtInBytes)
	if err != nil {
		fmt.Printf("cannot publish message", err)
		return err
	}

	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return nil
}
