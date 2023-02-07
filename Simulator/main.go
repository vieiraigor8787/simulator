package main
import (
		"fmt"
		"github.com/joho/godotenv"
		"log"
		"github.com/vieiraigor8787/simulator/Simulator/infra/kafka"
		kafka2 "github.com/vieiraigor8787/simulator/Simulator/application/kafka"
		ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("error loading .env file")
	}
}

func main() {
		msgChan := make(chan *ckafka.Message)
		consumer := kafka.NewKafkaConsumer(msgChan)
		go consumer.Consume()

		for msg := range msgChan {
			fmt.Println(string(msg.Value))
			go kafka2.Produce(msg)
		}
		// route := route2.Route{
		// 	ID: 			"1",
		// 	ClientID: "1",
		// };
		// route.LoadPositions()
		// stringjson, _ := route.ExportJsonPositions()
		// fmt.Println(stringjson[1])
}