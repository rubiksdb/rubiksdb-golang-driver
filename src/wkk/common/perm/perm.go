package perm

import (
	"wkk/common/misc"
)

const (
	FNV_OFFSET        = 14695981039346656037
	FNV_PRIME         = 1099511628211
	FNV_PRIME_INVERSE = 14886173955864302971
)

func Perm64(x uint64) uint64 {
	x += FNV_OFFSET
	x *= FNV_PRIME
	x ^= x >> 24
	x *= FNV_PRIME
	x ^= x >> 14
	x *= FNV_PRIME
	x ^= x >> 28
	return x
}

func step(x, n uint64) uint64 {
	t := x
	for b := n; b < 64; b += n {
		t = x ^ (t >> n)
	}
	return t
}

func Merp64(x uint64) uint64 {
	misc.Assert(((FNV_PRIME * FNV_PRIME_INVERSE) & 0xFFFFFFFFFFFFFFFF) == 1)

	x  = step(x, 28)
	x *= FNV_PRIME_INVERSE
	x  = step(x, 14)
	x *= FNV_PRIME_INVERSE
	x  = step(x, 24)
	x *= FNV_PRIME_INVERSE
	x -= FNV_OFFSET
	return x
}
