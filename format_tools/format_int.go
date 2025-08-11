package format_tools

import (
	// "fmt"
	"strconv"
	"strings"
	"text/scanner"
)

func FormatIntWithCommas(n int64) string {
	s := strconv.FormatInt(n, 10)

	var buf strings.Builder
	sc := scanner.Scanner{}
	sc.Init(strings.NewReader(s))
	sc.Mode = scanner.ScanInts

	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			buf.WriteRune(',')
		}
		buf.WriteRune(r)
	}

	// endString := buf.String()
	// endString = strings.TrimRight(endString, ",")
	// fmt.Println("Formatted number with commas:", endString)

	return buf.String()
}
