package format_tools

import (
	"fmt"
	"strings"
)

// 打印类似python prettytable的表格
func PrintPrettyTable(headers []string, rows [][]string) {
	// 计算每列最大宽度
	colWidths := make([]int, len(headers))
	for i, h := range headers {
		colWidths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// 构造分隔线
	sep := "+"
	for _, w := range colWidths {
		sep += strings.Repeat("-", w+2) + "+"
	}

	// 打印表头
	fmt.Println(sep)
	fmt.Print("|")
	for i, h := range headers {
		fmt.Printf(" %-*s |", colWidths[i], h)
	}
	fmt.Println()
	fmt.Println(sep)

	// 打印内容
	for _, row := range rows {
		fmt.Print("|")
		for i, cell := range row {
			fmt.Printf(" %-*s |", colWidths[i], cell)
		}
		fmt.Println()
	}
	fmt.Println(sep)
}
