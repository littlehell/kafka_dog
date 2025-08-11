package advanced_tools

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/manifoldco/promptui"
)

func InputInCmd(host *string, sha256Enabled, sha512Enabled *bool, username, password *string, listTopics, topicDetail,
	listConsumerGroups, consumerGroupsDetail *bool, topicKeyword, groupKeyword *string, testConsumeFromLatest *bool,
	groupTopicKeyword, topicName, groupName *string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("请输入Kafka Broker地址，格式为 ip:port:")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input == "" {
		fmt.Println("使用默认地址: 127.0.0.1:9091")
		input = "127.0.0.1:9091"
	}
	*host = input

	sslTypesChoices := []string{"SHA-256", "SHA-512", "PLAINTEXT"}
	prompt := promptui.Select{
		Label: "请选择一个选项",
		Items: sslTypesChoices,
		Size:  3,
		Templates: &promptui.SelectTemplates{
			Active:   `{{ "▸" | cyan }} {{ . | cyan }}`,
			Inactive: `  {{ . }}`,
			Selected: `{{ "✔" | green }} {{ . | green }}`,
		},
		Stdout: os.Stderr, // 避免在某些终端卡住
	}

	_, result, err := prompt.Run()
	if err != nil {
		fmt.Printf("选择失败: %v\n", err)
		return
	}

	if result == "SHA-256" {
		*sha256Enabled = true
		InputInCmdAuth(username, password)
	} else if result == "SHA-512" {
		*sha512Enabled = true
		InputInCmdAuth(username, password)
	} else {
		*sha256Enabled = false
		*sha512Enabled = false
	}

	ChoseOps(listTopics, topicDetail, listConsumerGroups, consumerGroupsDetail, topicKeyword, groupKeyword, groupTopicKeyword, testConsumeFromLatest, topicName, groupName)
}

func InputInCmdAuth(username, password *string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("请输入Kafka用户名:")
	inputUsername, _ := reader.ReadString('\n')
	*username = strings.TrimSpace(inputUsername)

	reader2 := bufio.NewReader(os.Stdin)
	fmt.Printf("请输入Kafka密码:")
	inputPassword, _ := reader2.ReadString('\n')
	*password = strings.TrimSpace(inputPassword)
}

func InputKeywords(keywords_type, topicKeyword, groupKeyword, groupTopicKeyword *string, listConsumerGroups *bool) {
	if *keywords_type == "topic" {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("请输入要查看的Topic关键词:")
		inputTopicKeyword, _ := reader.ReadString('\n')
		*topicKeyword = strings.TrimSpace(inputTopicKeyword)
	} else if *keywords_type == "group" && !*listConsumerGroups {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("请输入要查看的Consumer Group关键词:")
		inputGroupKeyword, _ := reader.ReadString('\n')
		*groupKeyword = strings.TrimSpace(inputGroupKeyword)

		if *groupTopicKeyword == "" {
			reader1 := bufio.NewReader(os.Stdin)
			fmt.Printf("请输入要查看的Consumer Group中Topic的关键词:")
			inputGroupTopicKeyword, _ := reader1.ReadString('\n')
			*groupTopicKeyword = strings.TrimSpace(inputGroupTopicKeyword)
		}
	} else if *keywords_type == "group" && *listConsumerGroups {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("请输入要查看的Consumer Group关键词:")
		inputGroupKeyword, _ := reader.ReadString('\n')
		*groupKeyword = strings.TrimSpace(inputGroupKeyword)
	}
}

func ChoseOps(listTopics, topicDetail, listConsumerGroups, consumerGroupsDetail *bool, topicKeyword, groupKeyword, groupTopicKeyword *string, testConsumeFromLatest *bool,
	topicName, groupName *string) {
	opsChoices := []string{"检查连接情况", "查看Topic列表", "查看Topic详情", "查看Consumer Group列表", "查看Consumer Group详情", "测试消费Topic"}
	prompt := promptui.Select{
		Label: "请选择一个选项",
		Items: opsChoices,
		Size:  6,
		Templates: &promptui.SelectTemplates{
			Active:   `{{ "▸" | cyan }} {{ . | cyan }}`,
			Inactive: `  {{ . }}`,
			Selected: `{{ "✔" | green }} {{ . | green }}`,
		},
		Stdout: os.Stderr, // 避免在某些终端卡住
	}

	_, result, err := prompt.Run()
	if err != nil {
		fmt.Printf("选择失败: %v\n", err)
		return
	}

	switch result {
	case "检查连接情况":
		// 什么都不做，直接返回
	case "查看Topic列表":
		*listTopics = true
		if *topicName == "" {
			keywords_type := "topic"
			InputKeywords(&keywords_type, topicKeyword, groupKeyword, groupTopicKeyword, listConsumerGroups)
		}
	case "查看Topic详情":
		*topicDetail = true
		if *topicName == "" {
			keywords_type := "topic"
			InputKeywords(&keywords_type, topicKeyword, groupKeyword, groupTopicKeyword, listConsumerGroups)
		}
	case "查看Consumer Group列表":
		*listConsumerGroups = true
		if *groupName == "" {
			keywords_type := "group"
			InputKeywords(&keywords_type, topicKeyword, groupKeyword, groupTopicKeyword, listConsumerGroups)
		}
	case "查看Consumer Group详情":
		*consumerGroupsDetail = true
		if *groupName == "" {
			keywords_type := "group"
			InputKeywords(&keywords_type, topicKeyword, groupKeyword, groupTopicKeyword, listConsumerGroups)
		}
	case "测试消费Topic":
		*testConsumeFromLatest = true
		keywords_type := "topic"
		InputKeywords(&keywords_type, topicKeyword, groupKeyword, groupTopicKeyword, listConsumerGroups)
	}
}
