package advanced_tools

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// CheckPort 检查远程服务器端口是否开放，hostPort格式为"host:port"
func CheckPort(hostPort string, timeoutSec int) bool {
	parts := strings.Split(hostPort, ":")
	if len(parts) != 2 {
		fmt.Println("输入格式错误，应为 host:port")
		return false
	}
	timeout := time.Duration(timeoutSec) * time.Second
	fmt.Printf("正在检查端口 %s 是否开放...\n", hostPort)
	conn, err := net.DialTimeout("tcp", hostPort, timeout)
	if err != nil {
		fmt.Printf("端口 %s 未开放\n", hostPort)
		fmt.Println(err)
		return false
	} else {
		fmt.Printf("端口 %s 已开放\n", hostPort)
	}
	defer conn.Close()
	return true
}
