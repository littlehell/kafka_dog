package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"kafka_dog/advanced_tools"
	"kafka_dog/consumer_tools"
	"kafka_dog/format_tools"
	"kafka_dog/topic_tools"

	"github.com/IBM/sarama"
	"github.com/fatih/color"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `
Kafka Dog - Kafka命令行工具

用法:
  on Windows: kafka_dog.exe [选项]
  on Linux:   kafka_dog_linux [选项]
  不添加任何选项进入命令行选择模式

常用选项:
  -host ip:port            Kafka地址, 只加host参数则测试连接情况
  -topic-list              查看Kafka topic，可使用-topic-keyword过滤
  -topic-name str		   输入topic名称查看详细信息，该参数会覆盖-topic-keyword参数(支持在命令行选择模式中使用)
  -topic-detail            选择某个topic查看详细信息，可使用-topic-keyword过滤
  -topic-keyword str       查看包含某个关键词的topic
  -group-list              查看所有Kafka消费组，可使用-group-keyword过滤
  -group-detail            查看某个消费组的详细信息，可使用-group-keyword过滤
  -group-name str          输入消费组名称查看详细信息，该参数会覆盖-group-keyword参数(支持在命令行选择模式中使用，会覆盖关键字参数)
  -group-keyword str       查看包含某个关键词的消费组
  -group-topic-keyword str 查看消费组包含某个关键词的topic(只支持与-group-detail一起使用)(支持在命令行选择模式中使用)
  -from-beginning int      选择某个topic，从头消费N条消息，可使用-topic-keyword过滤
  -from-latest             选择某个topic，从最新消费消息，可使用-topic-keyword过滤
  -sha-256                 是否启用SHA-256连接
  -sha-512                 是否启用SHA-512连接
  -usr str                 Kafka 认证用户名
  -pwd str                 Kafka 认证密码
  -h, --help               显示帮助信息

示例:
kafka_dog
kafka_dog -host 127.0.0.1:9092 -topic-list
kafka_dog -host 127.0.0.1:9092 -group-list
kafka_dog -host 127.0.0.1:9092 -sha-256 -usr admin -pwd 123456 -topic-list
kafka_dog -group-detail -group-name test -group-topic-keyword topicName

`)
	}

	host := flag.String("host", "", "Kafka地址, 只加host参数则测试连接情况")

	listTopics := flag.Bool("topic-list", false, "查看Kafka topic，可使用-topic-keyword参数过滤")
	topicName := flag.String("topic-name", "", "输入topic名称查看详细信息")
	topicDetail := flag.Bool("topic-detail", false, "选择某个topic查看详细信息")
	topicKeyword := flag.String("topic-keyword", "", "查看包含某个关键词的topic")

	listConsumerGroups := flag.Bool("group-list", false, "查看所有Kafka消费组")
	consumerGroupsDetail := flag.Bool("group-detail", false, "查看某个消费组的详细信息")
	groupName := flag.String("group-name", "", "查看输入名称的消费组")
	groupKeyword := flag.String("group-keyword", "", "查看包含某个关键词的消费组")
	groupTopicKeyword := flag.String("group-topic-keyword", "", "查看消费组包含某个关键词的topic")

	testConsumeFromBeginning := flag.Int("from-beginning", 0, "选择某个topic，从头消费N条消息")
	testConsumeFromLatest := flag.Bool("from-latest", false, "选择某个topic，从最新消费消息")

	// 如果需要TLS连接，可以添加相关参数
	sha256Enabled := flag.Bool("sha-256", false, "是否启用SHA-256连接")
	sha512Enabled := flag.Bool("sha-512", false, "是否启用SHA-512连接")

	// SASL/SCRAM-SHA-256认证相关参数
	username := flag.String("usr", "", "Kafka 认证用户名")
	password := flag.String("pwd", "", "Kafka 认证密码")

	flag.Parse()

	if *host == "" {
		advanced_tools.InputInCmd(host, sha256Enabled, sha512Enabled, username, password, listTopics, topicDetail,
			listConsumerGroups, consumerGroupsDetail, topicKeyword, groupKeyword, testConsumeFromLatest, groupTopicKeyword, topicName, groupName)
	}

	if !advanced_tools.ParameterCheck(listTopics, topicDetail, listConsumerGroups, consumerGroupsDetail,
		topicKeyword, groupKeyword, testConsumeFromBeginning, testConsumeFromLatest,
		sha256Enabled, sha512Enabled, username, password, groupTopicKeyword) {
		return
	}

	portOpen := advanced_tools.CheckPort(*host, 10) // 检查端口是否开放，默认Kafka端口为9092
	if !portOpen {
		color.Red("端口未开放或连接失败，请检查Kafka地址和端口是否正确")
		return
	} else {
		color.Green("✔端口已开放，连接成功")
	}

	// 指定 Kafka broker 地址
	brokers := []string{*host} // 替换为你的 Kafka 地址
	fmt.Println("正在连接kafka地址:", *host)

	if *sha256Enabled {
		if *username == "" || *password == "" {
			color.Red("启用SASL/SCRAM-SHA-256认证时，必须提供用户名和密码")
			return
		} else {
			ssl_type := "SASL/SCRAM-SHA-256"
			if !advanced_tools.CheckBrokerConnectionSHA(ssl_type, *host, *username, *password) {
				color.Red("连接kafka地址失败，请检查地址、用户名和密码是否正确")
				return
			} else {
				color.Green("✔连接SHA-256认证kafka地址成功")
				sha_ops(brokers, *username, *password, listTopics, topicDetail,
					listConsumerGroups, consumerGroupsDetail, topicKeyword, groupKeyword,
					testConsumeFromBeginning, testConsumeFromLatest, &ssl_type)
			}
		}
	} else if *sha512Enabled {
		if *username == "" || *password == "" {
			color.Red("启用SASL/SCRAM-SHA-512认证时，必须提供用户名和密码")
			return
		} else {
			ssl_type := "SASL/SCRAM-SHA-512"
			if !advanced_tools.CheckBrokerConnectionSHA(ssl_type, *host, *username, *password) {
				color.Red("连接kafka地址失败，请检查地址、用户名和密码是否正确")
				return
			} else {
				color.Green("✔连接SHA-512认证kafka地址成功")
				sha_ops(brokers, *username, *password, listTopics, topicDetail,
					listConsumerGroups, consumerGroupsDetail, topicKeyword, groupKeyword,
					testConsumeFromBeginning, testConsumeFromLatest, &ssl_type)
			}
		}
	} else {
		// 配置 Kafka
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		// 检查 Kafka broker 连接
		if !advanced_tools.CheckBrokerConnection(brokers, config) {
			color.Red("连接kafka地址失败，请检查地址是否正确")
			return
		} else {
			color.Green("✔连接PLAINTEXT认证kafka地址成功")
			plaintext_ops(listTopics, topicDetail, listConsumerGroups, consumerGroupsDetail,
				topicName, topicKeyword, groupKeyword, groupTopicKeyword, groupName, brokers, config, testConsumeFromBeginning, testConsumeFromLatest)
		}
	}
}

func plaintext_ops(listTopics, topicDetail, listConsumerGroups, consumerGroupsDetail *bool,
	topicName, topicKeyword, groupKeyword, groupTopicKeyword, groupName *string, brokers []string, config *sarama.Config,
	testConsumeFromBeginning *int, testConsumeFromLatest *bool) {
	if *listTopics {
		if *topicName != "" {
			*topicKeyword = *topicName
		}
		topic_tools.ShowTopics(brokers, config, *topicKeyword)
	}

	if *listConsumerGroups {
		if *groupName != "" {
			*groupKeyword = *groupName
		}
		groupNames, err := consumer_tools.GetAllConsumerGroups(brokers, config, *groupKeyword)
		if err != nil {
			color.Red("获取消费组失败:", err)
			return
		}
		fmt.Println("Kafka消费组列表:")
		for i, group := range groupNames {
			fmt.Printf("%d. %s\n", i+1, group)
		}
	}

	if *topicDetail {
		if *topicName != "" {
			topic_tools.TopicDetail(brokers, config, *topicName)
		} else {
			topic_map := topic_tools.ShowTopicsReturnMap(brokers, config, *topicKeyword)

			reader := bufio.NewReader(os.Stdin)
			var idx int

			for {
				fmt.Printf("请输入要查看topic对应的id(1-%d):", len(topic_map))
				input, _ := reader.ReadString('\n')
				input = strings.TrimSpace(input)
				num, err := strconv.Atoi(input)
				if err == nil && num > 0 && num <= len(topic_map) {
					idx = num
					break
				}
				color.Red("输入无效，请重新输入。")
			}
			fmt.Printf("查看第 %d 个 topic: %s\n", idx, topic_map[idx])

			topic_tools.TopicDetail(brokers, config, topic_map[idx])
		}

		return
	}

	if *consumerGroupsDetail {
		fmt.Println("消费组关键字:", *groupTopicKeyword)
		var selectedGroupName string
		if *groupName != "" {
			selectedGroupName = *groupName
		} else {
			groupNames, err := consumer_tools.GetAllConsumerGroups(brokers, config, *groupKeyword)
			if err != nil {
				color.Red("获取消费组失败:", err)
				return
			}
			fmt.Println("Kafka消费组列表:")
			for i, group := range groupNames {
				fmt.Printf("%d. %s\n", i+1, group)
			}

			reader := bufio.NewReader(os.Stdin)
			var idx int

			for {
				fmt.Printf("请输入要查看消费组对应的id(1-%d):", len(groupNames))
				input, _ := reader.ReadString('\n')
				input = strings.TrimSpace(input)
				num, err := strconv.Atoi(input)
				if err == nil && num > 0 && num <= len(groupNames) {
					idx = num
					break
				}
				color.Red("输入无效，请重新输入。")
			}
			fmt.Printf("查看第 %d 个消费组: %s\n", idx, groupNames[idx-1])
			selectedGroupName = groupNames[idx-1]
		}

		table, err := consumer_tools.GetConsumerGroupDetailsTable(brokers, config, selectedGroupName, *groupTopicKeyword)

		// if table[1][0] == "未找到消费组" {
		// 	fmt.Println("未找到消费组:", groupNames[idx-1])
		// 	return
		// } else if table[1][1] == "0" {
		// 	fmt.Println("消费组没有成员:", groupNames[idx-1])
		// 	return
		// }

		if err != nil {
			color.Red("获取消费组详情失败:", err)
			return
		} else {
			table_header := []string{"GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"}

			format_tools.PrintPrettyTable(table_header, table)
		}

		return
	}

	if *testConsumeFromBeginning > 0 {
		topic_map := topic_tools.ShowTopicsReturnMap(brokers, config, *topicKeyword)

		reader := bufio.NewReader(os.Stdin)
		var idx int

		for {
			fmt.Printf("请输入要消费的topic对应的id(1-%d):", len(topic_map))
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			num, err := strconv.Atoi(input)
			if err == nil && num > 0 && num <= len(topic_map) {
				idx = num
				break
			}
			color.Red("输入无效，请重新输入。")
		}
		fmt.Printf("从 %s topic 开始消费 %d 条消息\n", topic_map[idx], *testConsumeFromBeginning)

		err := consumer_tools.ConsumeFromBeginning(brokers, config, topic_map[idx], *testConsumeFromBeginning)
		if err != nil {
			color.Red("消费失败:", err)
		}
		return
	}

	if *testConsumeFromLatest {
		topic_map := topic_tools.ShowTopicsReturnMap(brokers, config, *topicKeyword)

		reader := bufio.NewReader(os.Stdin)
		var idx int

		for {
			fmt.Printf("请输入要消费的topic对应的id(1-%d):", len(topic_map))
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			num, err := strconv.Atoi(input)
			if err == nil && num > 0 && num <= len(topic_map) {
				idx = num
				break
			}
			color.Red("输入无效，请重新输入。")
		}
		// fmt.Println(*testConsumeFromLatest)
		fmt.Printf("从 %s topic 开始消费最新消息\n", topic_map[idx])

		err := consumer_tools.ConsumeFromLastest(brokers, config, topic_map[idx])
		if err != nil {
			color.Red("消费失败:", err)
		}
		return
	}
}

func sha_ops(brokers []string, username, password string,
	listTopics, topicDetail, listConsumerGroups, consumerGroupsDetail *bool,
	topicKeyword, groupKeyword *string, testConsumeFromBeginning *int, testConsumeFromLatest *bool, ssl_type *string) {
	if *listTopics {
		if *ssl_type == "SASL/SCRAM-SHA-256" {
			topic_tools.ShowTopicsSHA(brokers[0], username, password, "SASL/SCRAM-SHA-256", *topicKeyword)
		} else if *ssl_type == "SASL/SCRAM-SHA-512" {
			topic_tools.ShowTopicsSHA(brokers[0], username, password, "SASL/SCRAM-SHA-512", *topicKeyword)
		} else {
			color.Red("不支持的SSL类型:", *ssl_type)
			return
		}
	}

	if *topicDetail {
		topic_map := topic_tools.ShowTopicsSHAReturnMap(brokers[0], username, password, *ssl_type, *topicKeyword)

		reader := bufio.NewReader(os.Stdin)
		var idx int

		for {
			fmt.Printf("请输入要查看topic对应的id(1-%d):", len(topic_map))
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			num, err := strconv.Atoi(input)
			if err == nil && num > 0 && num <= len(topic_map) {
				idx = num
				break
			}
			color.Red("输入无效，请重新输入。")
		}
		fmt.Printf("查看第 %d 个 topic: %s\n", idx, topic_map[idx])

		topic_tools.TopicDetailSHA(brokers[0], username, password, *ssl_type, topic_map[idx])
		return
	}

	if *listConsumerGroups {
		groupNames, err := consumer_tools.GetAllConsumerGroupsSHA(brokers[0], username, password, *ssl_type, *groupKeyword)
		if err != nil {
			color.Red("未找到消费组")
			// fmt.Println("获取消费组失败:", err)
			return
		}
		// fmt.Println(groupNames)
		fmt.Println("Kafka消费组列表:")
		for i, group := range groupNames {
			fmt.Printf("%d. %s\n", i+1, group)
		}
	}

	if *consumerGroupsDetail {
		groupNames, err := consumer_tools.GetAllConsumerGroupsSHA(brokers[0], username, password, *ssl_type, *groupKeyword)

		if err != nil {
			color.Red("未找到消费组")
			// fmt.Println("获取消费组失败:", err)
			return
		}
		fmt.Println("Kafka消费组列表:")
		for i, group := range groupNames {
			fmt.Printf("%d. %s\n", i+1, group)
		}

		reader := bufio.NewReader(os.Stdin)
		var idx int

		for {
			fmt.Printf("请输入要查看消费组对应的id(1-%d):", len(groupNames))
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			num, err := strconv.Atoi(input)
			if err == nil && num > 0 && num <= len(groupNames) {
				idx = num
				break
			}
			color.Red("输入无效，请重新输入。")
			// fmt.Println("输入无效，请重新输入。")
		}
		fmt.Printf("查看第 %d 个消费组: %s\n", idx, groupNames[idx-1])

		table, err := consumer_tools.GetConsumerGroupDetailsTableSHA(brokers[0], username, password, *ssl_type, groupNames[idx-1])

		if table[1][0] == "未找到消费组" {
			color.Red("未找到消费组:", groupNames[idx-1])
			// fmt.Println("未找到消费组:", groupNames[idx-1])
			return
		} else if table[1][1] == "0" {
			color.Red("消费组没有成员:", groupNames[idx-1])
			// fmt.Println("消费组没有成员:", groupNames[idx-1])
			return
		}

		if err != nil {
			fmt.Println("获取消费组详情失败:", err)
			return
		} else {
			table_header := []string{"GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"}

			format_tools.PrintPrettyTable(table_header, table)
		}

		return
	}

	if *testConsumeFromBeginning > 0 { // 从头消费消息
		topic_map := topic_tools.ShowTopicsSHAReturnMap(brokers[0], username, password, *ssl_type, *topicKeyword)

		reader := bufio.NewReader(os.Stdin)
		var idx int

		for {
			fmt.Printf("请输入要消费的topic对应的id(1-%d):", len(topic_map))
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			num, err := strconv.Atoi(input)
			if err == nil && num > 0 && num <= len(topic_map) {
				idx = num
				break
			}
			color.Red("输入无效，请重新输入。")
			// fmt.Println("输入无效，请重新输入。")
		}
		fmt.Printf("从 %s topic 开始消费 %d 条消息\n", topic_map[idx], *testConsumeFromBeginning)

		err := consumer_tools.ConsumeFromBeginningSHA(brokers[0], username, password, *ssl_type, topic_map[idx], *testConsumeFromBeginning)
		if err != nil {
			color.Red("消费失败:", err)
			// fmt.Println("消费失败:", err)
		}
		return
	}

	if *testConsumeFromLatest {
		topic_map := topic_tools.ShowTopicsSHAReturnMap(brokers[0], username, password, *ssl_type, *topicKeyword)

		reader := bufio.NewReader(os.Stdin)
		var idx int

		for {
			fmt.Printf("请输入要消费的topic对应的id(1-%d):", len(topic_map))
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			num, err := strconv.Atoi(input)
			if err == nil && num > 0 && num <= len(topic_map) {
				idx = num
				break
			}
			color.Red("输入无效，请重新输入。")
			// fmt.Println("输入无效，请重新输入。")
		}
		fmt.Printf("从 %s topic 开始消费最新消息\n", topic_map[idx])

		err := consumer_tools.ConsumeFromLatestSHA(brokers[0], username, password, *ssl_type, topic_map[idx])
		if err != nil {
			color.Red("消费失败:", err)
			// fmt.Println("消费失败:", err)
		}
		return
	}
}
