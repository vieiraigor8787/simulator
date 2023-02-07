package kafka

import (
		"github.com/vieiraigor8787/fullcycle-simulator-igor/infra/kafka"
		 ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
		 route2 "github.com/vieiraigor8787/fullcycle-simulator-igor/application/route"
		"log"
		"os"
		"time"
		"encoding/json"
)

func Produce(msg *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	route := route2.NewRoute()
	json.Unmarshal(msg.Value, &route)
	route.LoadPositions()
	positions, err := route.ExportJsonPositions()
	if err != nil {
		log.Println(err.Error())
	}
	for _, p := range positions {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer)
		time.Sleep(time.Millisecond * 5000)
	}
}