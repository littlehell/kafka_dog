package topic_tools

import (
	"fmt"
	"kafka_dog/format_tools"
	"log"
	"strings"

	"github.com/IBM/sarama"
)

func ShowTopics(brokers []string, config *sarama.Config, keyword string) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Error creating Kafka client: %v", err)
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Error fetching topics: %v", err)
	}

	fmt.Println("Kafka Topics:")
	topic_index := 1
	for _, topic := range topics {
		if keyword != "" && !strings.Contains(strings.ToLower(topic), strings.ToLower(keyword)) {
			continue
		} else {
			fmt.Println(topic_index, " -", topic)
			topic_index += 1
		}
	}
}

func ShowTopicsReturnMap(brokers []string, config *sarama.Config, keyword string) map[int]string {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Error creating Kafka client: %v", err)
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Error fetching topics: %v", err)
	}

	topicMap := make(map[int]string)

	fmt.Println("Kafka Topics:")
	topic_index := 1
	for _, topic := range topics {
		if keyword != "" && !strings.Contains(strings.ToLower(topic), strings.ToLower(keyword)) {
			continue
		} else {
			fmt.Println(topic_index, " -", topic)
			topicMap[topic_index] = topic
			topic_index += 1
		}
	}
	return topicMap
}

func TopicDetail(brokers []string, config *sarama.Config, topic string) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		fmt.Printf("Error creating Kafka client: %v", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		fmt.Printf("Error fetching partitions for topic %s: %v", topic, err)
	}

	fmt.Printf("Topic名称: %s\n", topic)
	fmt.Printf("Partition数量: %d\n", len(partitions))

	totalMessages := int64(0)
	for _, partition := range partitions {
		oldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			fmt.Printf("Error getting oldest offset for partition %d: %v", partition, err)
			continue
		}
		newest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("Error getting newest offset for partition %d: %v", partition, err)
			continue
		}
		count := newest - oldest
		totalMessages += count

		newestFormatted := format_tools.FormatIntWithCommas(newest)
		oldestFormatted := format_tools.FormatIntWithCommas(oldest)
		countFormatted := format_tools.FormatIntWithCommas(count)

		fmt.Printf("  Partition %d: oldest offset = %s | newest offset = %s | message count = %s\n", partition, oldestFormatted, newestFormatted, countFormatted)
	}
	totalMessagesFormatted := format_tools.FormatIntWithCommas(totalMessages)
	fmt.Printf("Topic'%s'中总消息数量: %s\n", topic, totalMessagesFormatted)
}
