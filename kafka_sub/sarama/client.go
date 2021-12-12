package sarama

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	sarama "github.com/Shopify/sarama"
)

type ConsumerGroupHandler struct {
	ready chan bool
}

func (c *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	session.ResetOffset("t2p4", 0, 13, "")
	// log.Info(session.Claims())
	// Mark the consumer as ready
	close(c.ready)
	return nil
}
func (c *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (c *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("this consumer's partition: %d\n", claim.Partition())
	for msg := range claim.Messages() {
		// fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		fmt.Printf("Message:key:%s, value:%s,partition: %d \n", string(msg.Key), string(msg.Value), msg.Partition)
		session.MarkMessage(msg, "")
		session.Commit()
	}
	return nil
}

const (
	addr    = "192.168.3.12:9092"
	topic   = "test"
	groupid = "test-consumer-group"
)

func newConfig() *sarama.Config {
	rand.Seed(time.Now().UnixNano())
	config := sarama.NewConfig()
	config.ClientID = fmt.Sprintf("go-kafka-consumer-%d", rand.Int())
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.ChannelBufferSize = 2
	config.Consumer.Offsets.Initial = -2
	config.Consumer.Return.Errors = true
	fmt.Printf("consumer group client: %s\n", config.ClientID)
	return config
}

func newClient(brokers []string, config *sarama.Config) *sarama.Client {

	client, err := sarama.NewClient(brokers, config)

	if err != nil {
		fmt.Printf("Error creating client: %v\n", err)
	}

	return &client
}

func newConsumerGroup(client *sarama.Client, group string) *sarama.ConsumerGroup {

	consumerGroup, err := sarama.NewConsumerGroupFromClient(group, *client)
	if err != nil {
		fmt.Printf("Error creating consumer group: %v\n", err)
	}

	return &consumerGroup
}

// 使用sarama订阅消息
func Subscribe() {
	// Specify brokers address. This is default one
	brokers := []string{addr}
	client := newClient(brokers, newConfig())

	defer (*client).Close()

	consumerGroup := newConsumerGroup(client, groupid)

	defer (*consumerGroup).Close()

	ctx, cancel := context.WithCancel(context.Background())
	handler := &ConsumerGroupHandler{
		ready: make(chan bool),
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err := (*consumerGroup).Consume(ctx, []string{topic}, handler)
			if err != nil {
				fmt.Printf("Error from consumer: %v\n", err)
				return
			}
			handler.ready = make(chan bool)
			fmt.Printf("waiting for consumer to be ready\n")
		}
	}()
	<-handler.ready

	wg.Wait()
	cancel()
}
