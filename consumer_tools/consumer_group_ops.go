package consumer_tools

import (
	"fmt"
	"strings"

	"github.com/IBM/sarama"
)

// 获取所有消费组
func GetAllConsumerGroups(brokers []string, config *sarama.Config, keyword string) ([]string, error) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}
	defer admin.Close()

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		return nil, err
	}

	groupNames := make([]string, 0, len(groups))
	for group := range groups {
		if keyword != "" && !strings.Contains(group, keyword) {
			continue
		} else {
			groupNames = append(groupNames, group)
		}
	}
	return groupNames, nil
}

func GetConsumerGroupDetailsTable(brokers []string, config *sarama.Config, group, groupTopicKeyword string) ([][]string, error) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}
	defer admin.Close()

	// 获取消费组描述
	desc, err := admin.DescribeConsumerGroups([]string{group})
	if err != nil {
		return nil, err
	}
	if len(desc) == 0 {
		return nil, fmt.Errorf("未找到消费组: %s", group)
	}
	// fmt.Println("消费组描述:", desc[0])
	// 获取消费组分配的topic/partition
	offsets, err := admin.ListConsumerGroupOffsets(group, nil)
	if err != nil {
		return nil, err
	}
	// fmt.Println("消费组分配的topic/partition:", offsets.Blocks)
	var table [][]string

	for topic, partitions := range offsets.Blocks {
		for partition, block := range partitions {
			// 获取log-end-offset
			endOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				endOffset = -1
			}
			lag := int64(-1)
			if block.Offset >= 0 && endOffset >= 0 {
				lag = endOffset - block.Offset
			}

			consumerID := ""
			host := ""
			clientID := ""
			// 查找member信息
			for _, groupDesc := range desc {
				for _, member := range groupDesc.Members {
					assignment, err := member.GetMemberAssignment()
					if err != nil {
						continue
					}
					if assignment == nil {
						continue
					}
					for t, ps := range assignment.Topics {
						if t == topic {
							for _, p := range ps {
								if int32(p) == partition {
									consumerID = member.ClientId
									host = member.ClientHost
									clientID = member.MemberId
								}
							}
						}
					}
				}
			}

			row := []string{
				group,
				topic,
				fmt.Sprintf("%d", partition),
				fmt.Sprintf("%d", block.Offset),
				fmt.Sprintf("%d", endOffset),
				fmt.Sprintf("%d", lag),
				clientID,
				host,
				consumerID,
			}

			if groupTopicKeyword != "" && !strings.Contains(topic, groupTopicKeyword) {
				continue
			} else {
				table = append(table, row)
			}
			// table = append(table, row)
		}
	}
	return table, nil
}
