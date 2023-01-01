package siphash

import (
	"wkk/common/serd"
)

type Tweak struct {
	T [2]uint64
}

var DefaultTweak Tweak = Tweak{
	T: [2]uint64{0x967af6cfdf9eee0d, 0x65d44548a2b4df17},
}

func rotl64(v uint64, n uint) uint64 {
	return (v << n) | (v >> (64 - n))
}

func round(v0, v1, v2, v3 uint64) (uint64, uint64, uint64, uint64) {
	v0 ^= v1
	v1 = rotl64(v1, 13)
	v1 ^= v0
	v0 = rotl64(v0, 32)
	v2 ^= v3
	v3 = rotl64(v3, 16)
	v3 ^= v2
	v0 ^= v3
	v3 = rotl64(v3, 21)
	v3 ^= v0
	v2 ^= v1
	v1 = rotl64(v1, 17)
	v1 ^= v2
	v2 = rotl64(v2, 32)
	return v0, v1, v2, v3
}

func Siphash(data []byte, tweak Tweak) uint64 {
	b, x := uint64(len(data)) << 56, uint64(0)

	v0 := 0x736F6D6570736575 ^ tweak.T[0]
	v1 := 0x646F72616E646F6D ^ tweak.T[1]
	v2 := 0x6C7967656E657261 ^ tweak.T[0]
	v3 := 0x7465646279746573 ^ tweak.T[1]

	for len(data) >= 8 {
		x, data, _ = serd.Get64LE(8, data)
		v3 ^= x
		v0, v1, v2, v3 = round(v0, v1, v2, v3)
		v0, v1, v2, v3 = round(v0, v1, v2, v3)
		v0 ^= x
	}

	if len(data) > 0 {
		x, data, _ = serd.Get64LE(len(data), data)
		b |= x
	}

	v3 ^= b
	v0, v1, v2, v3 = round(v0, v1, v2, v3)
	v0, v1, v2, v3 = round(v0, v1, v2, v3)
	v0 ^= b

	v2 ^= 0xFF
	v0, v1, v2, v3 = round(v0, v1, v2, v3)
	v0, v1, v2, v3 = round(v0, v1, v2, v3)
	v0, v1, v2, v3 = round(v0, v1, v2, v3)
	v0, v1, v2, v3 = round(v0, v1, v2, v3)

	return v0 ^ v1 ^ v2 ^ v3
}
