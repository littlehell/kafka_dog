package advanced_tools

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/fatih/color"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func CheckBrokerConnection(brokers []string, config *sarama.Config) bool {
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		color.Red("连接Kafka Broker失败: %v", err)
		// fmt.Printf("Failed to connect to Kafka broker: %v\n", err)
		return false
	}
	defer client.Close()
	return true
}

func CheckBrokerConnectionSHA(ssl_type, broker, username, password string) bool {
	var (
		mechanism sasl.Mechanism
		err       error
	)

	switch ssl_type {
	case "SASL/SCRAM-SHA-256":
		mechanism, err = scram.Mechanism(scram.SHA256, username, password)
	case "SASL/SCRAM-SHA-512":
		mechanism, err = scram.Mechanism(scram.SHA512, username, password)
	default:
		color.Red("不支持的SSL类型: %s", ssl_type)
		return false
	}

	if err != nil {
		color.Red("连接服务器失败,错误: %v", err)
		return false
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		SASLMechanism: mechanism,
	}

	conn, err := dialer.DialContext(context.Background(), "tcp", broker)
	if err != nil {
		color.Red("连接服务器 %s 失败,错误: %v\n", broker, err)
		return false
	}
	defer conn.Close()

	return true
}
