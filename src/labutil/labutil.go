package labutil

import (
	"fmt"
	"math/rand"
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// random range
func RandRange(from, to int) int {
	return rand.Intn(to-from) + from
}

// convert int to subscript, e.g. 12 -> ₁₂
func ToSubscript(i int) (r string) {
	for r, i = fmt.Sprintf("%c", 8320+(i%10)), i/10; i != 0; r, i = fmt.Sprintf("%c", 8320+(i%10))+r, i/10 {
	}
	return
}

func Suffix(s string, cnt int) string {
	return s[Max(0, len(s)-cnt):]
}
