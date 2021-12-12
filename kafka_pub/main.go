package main

import (
	"fmt"

	"github.com/choby/mq_golang/kafka_pub/sarama"
)

func main() {
	fmt.Println("golang kafka producer")

	sarama.Publish()

}
