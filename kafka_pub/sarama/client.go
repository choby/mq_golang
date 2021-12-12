package sarama

import (
	"fmt"

	"github.com/Shopify/sarama"
)

const (
	addr  = "192.168.3.12:9092"
	topic = "test"
)

func newConfig() *sarama.Config {
	config := sarama.NewConfig()
	// config.ClientID = "go-kafka-producer"
	// config.Producer.RequiredAcks = sarama.WaitForAll
	//这个地方根据key来分区，并不能保证消息被平均分配到每个分区
	//比如假设一共有6个分区，消息key按 num%6 取模，key-3、key-5会被分配到同一个分区，key-2、key-4会被分配到另一个分区
	//具体原因待查
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	return config
}

func newMessage() sarama.ProducerMessage {
	message := sarama.ProducerMessage{}
	message.Topic = topic
	return message

}

func newProducer() *sarama.SyncProducer {
	brokers := []string{addr}
	config := newConfig()
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return nil
	}
	return &producer
}

func Publish() {
	i := 1
	producer := newProducer()

	for {

		message := newMessage()

		key := fmt.Sprintf("key-%d", i%6)
		message.Value = sarama.StringEncoder(fmt.Sprintf("this is a test message key: %s", key))
		message.Key = sarama.StringEncoder(key)

		partition, offset, err := (*producer).SendMessage(&message)

		if err != nil {
			fmt.Println("send message failed, err:", err)
			return
		}

		fmt.Printf("partition:%d, offset:%d,message: %s\n", partition, offset, message.Value)
		i++
	}
}
