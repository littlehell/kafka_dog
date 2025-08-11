package consumer_tools

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// 从头消费X条消息
func ConsumeFromBeginning(brokers []string, config *sarama.Config, topic string, count int) error {
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return fmt.Errorf("创建consumer失败: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return fmt.Errorf("获取分区失败: %v", err)
	}

	msgRead := 0
	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			continue
		}
		defer pc.Close()

		for msgRead < count {
			select {
			case msg := <-pc.Messages():
				fmt.Printf("partition=%d offset=%d\n", msg.Partition, msg.Offset)
				fmt.Printf("key=%s\n", string(msg.Key))
				fmt.Printf("message=%s\n", string(msg.Value))
				fmt.Printf("timestamp=%v\n", msg.Timestamp)
				fmt.Println("-----")
				msgRead++
				if msgRead >= count {
					return nil
				}
			case <-time.After(2 * time.Second):
				return nil
			}
		}
	}
	return nil
}

// 从最新持续消费消息，按Ctrl+C优雅退出
func ConsumeFromLastest(brokers []string, config *sarama.Config, topic string) error {
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return fmt.Errorf("创建consumer失败: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return fmt.Errorf("获取分区失败: %v", err)
	}

	// 信号处理
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})
	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			continue
		}
		go func(pc sarama.PartitionConsumer) {
			defer pc.Close()
			for {
				select {
				case msg := <-pc.Messages():
					fmt.Printf("partition=%d offset=%d\n", msg.Partition, msg.Offset)
					fmt.Printf("key=%s\n", string(msg.Key))
					fmt.Printf("message=%s\n", string(msg.Value))
					fmt.Printf("timestamp=%v\n", msg.Timestamp)
					fmt.Println("-----")
				case <-done:
					return
				}
			}
		}(pc)
	}

	<-sigchan
	close(done)
	fmt.Println("收到退出信号，优雅退出。")
	return nil
}
