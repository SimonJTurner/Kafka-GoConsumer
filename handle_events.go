package main

import (
	"encoding/json"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// PurchaseEvent tells of an item being purchased
type PurchaseEvent struct {
	ItemPurchased string `json:"itemPurchased"`
	TimeStamp     string `json:"timeStamp"`
}

// Inventory Service
func main() {
	inventory := map[string]int{
		"Product0": 600,
		"Product1": 600,
		"Product2": 600,
		"Product3": 600,
		"Product4": 600,
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test-consumer-group-inventory",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	fmt.Println("Service Started, subscribing now")
	c.SubscribeTopics([]string{"test"}, nil)
	fmt.Println("Service Subscribed, waiting for messages")

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
		var purchase PurchaseEvent
		err = json.Unmarshal(msg.Value, &purchase)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
		inventory[purchase.ItemPurchased] = inventory[purchase.ItemPurchased] - 1
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		fmt.Println("Inventory:")
		printMap(inventory)
	}

	c.Close()
}

func printMap(inventory map[string]int) {
	for k, v := range inventory {
		fmt.Println("\t[", k, " quantity: ", v, "]")
	}

}
