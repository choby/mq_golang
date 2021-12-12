package main

import (
	"fmt"

	"github.com/choby/mq_golang/kafka_sub/sarama"
)

func main() {
	fmt.Print("golang kafka consumer\n")

	sarama.Subscribe()

}
