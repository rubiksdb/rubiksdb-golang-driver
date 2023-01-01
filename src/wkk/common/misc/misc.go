package misc

import (
	"fmt"
	"time"
)

func Assert(cond bool)  {
	if !cond {
		panic("Assert")
	}
}

func AssertNilError(err error)  {
	if err != nil {
		panic(fmt.Sprintf("err=%s", err))
	}
}

func Due(deadline time.Time) bool {
	return time.Now().After(deadline)
}

func Bound(d1, d2 time.Time) time.Time {
	if d1.After(d2) {
		return d2
	}
	return d1
}

var PoisonBytes = []byte{0xa5, 0xa5, 0xa5, 0xa, 0xa5, 0xa5, 0xa5, 0xa5}

func Poison(dst []byte, partial bool)  {
	n := len(dst) / 8
	if partial {
		n = MinInt(len(dst), 128) / 8
	}

	for i := 0; i < n; i += 1 {
		copy(dst[i*8: ], PoisonBytes)
	}
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}