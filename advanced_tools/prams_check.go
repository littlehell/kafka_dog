package advanced_tools

import (
	"github.com/fatih/color"
)

func ParameterCheck(listTopics, topicDetail, listConsumerGroups, consumerGroupsDetail *bool,
	topicKeyword, groupKeyword *string, testConsumeFromBeginning *int, testConsumeFromLatest *bool,
	sha256Enabled *bool, sha512Enabled *bool, username *string, password, groupTopicKeyword *string) bool {
	// 互斥参数检测
	// topic-list/topic-detail/group-list/group-detail 互斥
	mainOps := 0
	if *listTopics {
		mainOps++
	}
	if *topicDetail {
		mainOps++
	}
	if *listConsumerGroups {
		mainOps++
	}
	if *consumerGroupsDetail {
		mainOps++
	}
	if *testConsumeFromBeginning > 0 {
		mainOps++
	}
	if *testConsumeFromLatest {
		mainOps++
	}
	if mainOps > 1 {
		color.Red("参数冲突：-topic-list、-topic-detail、-group-list、-group-detail、-from-beginning、-from-latest 只能选择一个")
		return false
	}
	// from-beginning/from-latest 互斥
	if *testConsumeFromBeginning > 0 && *testConsumeFromLatest {
		color.Red("参数冲突：-from-beginning 和 -from-latest 只能选择一个")
		return false
	}

	if !*listTopics && !*topicDetail && !*testConsumeFromLatest && topicKeyword != nil && *topicKeyword != "" {
		color.Red("参数错误：-topic-keyword 只能在 -topic-list, -topic-detail 或 -from-latest时使用")
		return false
	}

	if !*listConsumerGroups && !*consumerGroupsDetail && groupKeyword != nil && *groupKeyword != "" {
		color.Red("参数错误：-group-keyword 只能在 -group-list 或 -group-detail 时使用")
		return false
	}
	if *sha256Enabled && *sha512Enabled {
		color.Red("参数冲突：-sha-256 和 -sha-512 只能选择一个")
		return false
	}
	// if (!*sha256Enabled && !*sha512Enabled) && (*username != "" || *password != "") {
	// 	color.Red("参数错误：如果提供了用户名或密码，必须启用 -sha-256 或 -sha-512")
	// 	return false
	// }

	return true
}
