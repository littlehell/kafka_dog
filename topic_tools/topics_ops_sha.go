package topic_tools

import (
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// 支持SASL/SCRAM-SHA-256和SHA-512认证的topic查看

func ShowTopicsSHA(broker, username, password, sslType, keyword string) {
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
		fmt.Printf("unsupported sslType: %s\n", sslType)
		return
	}
	if err != nil {
		fmt.Printf("failed to create SCRAM mechanism: %v\n", err)
		return
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
	}

	conn, err := dialer.Dial("tcp", broker)
	if err != nil {
		fmt.Printf("failed to connect to broker %s: %v\n", broker, err)
		return
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		fmt.Printf("failed to read partitions: %v\n", err)
		return
	}

	topicSet := make(map[string]struct{})
	for _, p := range partitions {
		topicSet[p.Topic] = struct{}{}
	}

	fmt.Println("Kafka Topics:")
	topic_index := 1
	for topic := range topicSet {
		if keyword != "" && !strings.Contains(strings.ToLower(topic), strings.ToLower(keyword)) {
			continue
		}
		fmt.Println(topic_index, " -", topic)
		topic_index++
	}
}

func ShowTopicsSHAReturnMap(broker, username, password, sslType, keyword string) map[int]string {
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
		fmt.Printf("unsupported sslType: %s\n", sslType)
		return nil
	}
	if err != nil {
		fmt.Printf("failed to create SCRAM mechanism: %v\n", err)
		return nil
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
	}

	conn, err := dialer.Dial("tcp", broker)
	if err != nil {
		fmt.Printf("failed to connect to broker %s: %v\n", broker, err)
		return nil
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		fmt.Printf("failed to read partitions: %v\n", err)
		return nil
	}

	topicSet := make(map[string]struct{})
	for _, p := range partitions {
		topicSet[p.Topic] = struct{}{}
	}

	topicMap := make(map[int]string)

	fmt.Println("Kafka Topics:")
	topic_index := 1
	for topic := range topicSet {
		if keyword != "" && !strings.Contains(strings.ToLower(topic), strings.ToLower(keyword)) {
			continue
		}
		fmt.Println(topic_index, " -", topic)
		topicMap[topic_index] = topic
		topic_index++
	}
	return topicMap
}

func TopicDetailSHA(broker, username, password, sslType, topic string) {
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
		fmt.Printf("unsupported sslType: %s\n", sslType)
		return
	}
	if err != nil {
		fmt.Printf("failed to create SCRAM mechanism: %v\n", err)
		return
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
	}

	conn, err := dialer.Dial("tcp", broker)
	if err != nil {
		fmt.Printf("failed to connect to broker %s: %v\n", broker, err)
		return
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		fmt.Printf("failed to read partitions: %v\n", err)
		return
	}

	found := false
	for _, p := range partitions {
		if p.Topic == topic {
			found = true
			fmt.Printf("Topic名称: %s\n", topic)
			fmt.Printf("kafka总分区数: %d\n", len(partitions))
			fmt.Printf("分区ID: %d\n", p.ID)
			fmt.Printf("Leader Broker ID: %d\n", p.Leader.ID)
			fmt.Printf("副本 Broker IDs: ")
			for _, replica := range p.Replicas {
				fmt.Printf("%d ", replica.ID)
			}
			fmt.Println()
			break
		}
	}

	if !found {
		fmt.Printf("Topic '%s' not found.\n", topic)
	}

	var topicPartitions []kafka.Partition
	for _, p := range partitions {
		if p.Topic == topic {
			topicPartitions = append(topicPartitions, p)
		}
	}

	if len(topicPartitions) == 0 {
		fmt.Printf("Topic '%s' not found.\n", topic)
		return
	}

	fmt.Printf("Topic名称: %s\n", topic)
	fmt.Printf("分区数: %d\n", len(topicPartitions))
}
