package consumer_tools

import (
	"context"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// 获取支持SASL/SCRAM-SHA-256和SHA-512认证的所有消费者组，返回map[int]string
func GetAllConsumerGroupsSHA(broker, username, password, sslType, keyword string) (map[int]string, error) {
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
		return nil, fmt.Errorf("unsupported sslType: %s", sslType)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create SCRAM mechanism: %v", err)
	}

	transport := &kafka.Transport{
		SASL: mechanism,
	}

	client := kafka.Client{
		Addr:      kafka.TCP(broker),
		Transport: transport,
	}

	groups, err := client.ListGroups(context.Background(), &kafka.ListGroupsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %v", err)
	}

	groupMap := make(map[int]string)
	idx := 0
	for _, group := range groups.Groups {
		if keyword != "" && !strings.Contains(group.GroupID, keyword) {
			continue
		} else {
			groupMap[idx] = group.GroupID
			idx++
		}
	}
	return groupMap, nil
}

// 获取支持SASL/SCRAM-SHA-256和SHA-512认证的某个消费者组详情，输入参数为group名称，返回[][]string table
func GetConsumerGroupDetailsTableSHA(broker, username, password, sslType, group string) ([][]string, error) {
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
		return nil, fmt.Errorf("unsupported sslType: %s", sslType)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create SCRAM mechanism: %v", err)
	}

	transport := &kafka.Transport{
		SASL: mechanism,
	}

	client := kafka.Client{
		Addr:      kafka.TCP(broker),
		Transport: transport,
	}

	// DescribeGroups 获取组详情
	resp, err := client.DescribeGroups(context.Background(), &kafka.DescribeGroupsRequest{
		GroupIDs: []string{group},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe group: %v", err)
	}
	if len(resp.Groups) == 0 {
		return nil, fmt.Errorf("group not found: %s", group)
	}

	groupInfo := resp.Groups[0]
	table := [][]string{}

	// 添加组基本信息
	table = append(table, []string{"GroupID", groupInfo.GroupID})
	// 移除 State 和 ProtocolType 字段的添加，因为 kafka.DescribeGroupsResponseGroup 没有这些字段
	table = append(table, []string{"MembersCount", fmt.Sprintf("%d", len(groupInfo.Members))})

	// 添加成员信息
	for _, member := range groupInfo.Members {
		row := []string{
			"MemberID: " + member.MemberID,
			"ClientID: " + member.ClientID,
			"ClientHost: " + member.ClientHost,
		}
		table = append(table, row)
	}

	return table, nil
}
