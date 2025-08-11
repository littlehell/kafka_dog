package consumer_tools

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// 从头消费sha-256或sha-512认证kafka中给定topic的前N条消息
func ConsumeFromBeginningSHA(broker, username, password, sslType, topic string, count int) error {
	var (
		mechanism sasl.Mechanism
		err       error
	)
	switch sslType {
	case "SASL/SCRAM-SHA-256":
		mechanism, err = scram.Mechanism(scram.SHA256, username, password)
	case "SASL/SCRAM-SHA-512":
		mechanism, err = scram.Mechanism(scram.SHA512, username, password)
	default:
		return fmt.Errorf("unsupported sslType: %s", sslType)
	}
	if err != nil {
		return fmt.Errorf("failed to create SCRAM mechanism: %v", err)
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		SASLMechanism: mechanism,
	}

	// 获取分区列表
	conn, err := dialer.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("failed to connect to broker %s: %v", broker, err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %v", err)
	}

	var topicPartitions []int
	for _, p := range partitions {
		if p.Topic == topic {
			topicPartitions = append(topicPartitions, p.ID)
		}
	}
	if len(topicPartitions) == 0 {
		return fmt.Errorf("topic '%s' not found", topic)
	}

	msgRead := 0
	for _, partition := range topicPartitions {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{broker},
			Topic:       topic,
			Partition:   partition,
			MinBytes:    1,
			MaxBytes:    10e6,
			Dialer:      dialer,
			StartOffset: kafka.FirstOffset,
		})
		defer r.Close()

		for msgRead < count {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			m, err := r.ReadMessage(ctx)
			cancel()
			if err != nil {
				break
			}
			fmt.Printf("partition=%d offset=%d\n", m.Partition, m.Offset)
			fmt.Printf("key=%s\n", string(m.Key))
			fmt.Printf("message=%s\n", string(m.Value))
			fmt.Printf("timestamp=%v\n", m.Time)
			fmt.Println("-----")
			msgRead++
			if msgRead >= count {
				return nil
			}
		}
	}
	return nil
}

// 实时消费sha-256或sha-512认证kafka中给定topic的消息，按ctrl+c优雅退出
func ConsumeFromLatestSHA(broker, username, password, sslType, topic string) error {
	var (
		mechanism sasl.Mechanism
		err       error
	)
	switch sslType {
	case "SASL/SCRAM-SHA-256":
		mechanism, err = scram.Mechanism(scram.SHA256, username, password)
	case "SASL/SCRAM-SHA-512":
		mechanism, err = scram.Mechanism(scram.SHA512, username, password)
	default:
		return fmt.Errorf("unsupported sslType: %s", sslType)
	}
	if err != nil {
		return fmt.Errorf("failed to create SCRAM mechanism: %v", err)
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		SASLMechanism: mechanism,
	}

	// 获取分区列表
	conn, err := dialer.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("failed to connect to broker %s: %v", broker, err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %v", err)
	}

	var topicPartitions []int
	for _, p := range partitions {
		if p.Topic == topic {
			topicPartitions = append(topicPartitions, p.ID)
		}
	}
	if len(topicPartitions) == 0 {
		return fmt.Errorf("topic '%s' not found", topic)
	}

	// 信号处理
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	done := make(chan struct{})

	for _, partition := range topicPartitions {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{broker},
			Topic:       topic,
			Partition:   partition,
			MinBytes:    1,
			MaxBytes:    10e6,
			Dialer:      dialer,
			StartOffset: kafka.LastOffset,
		})

		go func(r *kafka.Reader, partition int) {
			defer r.Close()
			for {
				select {
				case <-done:
					return
				default:
					m, err := r.ReadMessage(context.Background())
					if err != nil {
						time.Sleep(500 * time.Millisecond)
						continue
					}
					fmt.Printf("partition=%d offset=%d\n", m.Partition, m.Offset)
					fmt.Printf("key=%s\n", string(m.Key))
					fmt.Printf("message=%s\n", string(m.Value))
					fmt.Printf("timestamp=%v\n", m.Time)
					fmt.Println("-----")
				}
			}
		}(r, partition)
	}

	<-sigchan
	close(done)
	fmt.Println("收到退出信号，优雅退出。")
	return nil
}
