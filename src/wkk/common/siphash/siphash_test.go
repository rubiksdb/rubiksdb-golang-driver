package siphash

import (
	"testing"
	"wkk/common/misc"
)

var (
	tweak = Tweak{T: [2]uint64{0x0706050403020100, 0x0f0e0d0c0b0a0908}}

	expected = []uint64{
		0x2231a79b14d64fc1,
		0x47ac8edd63640fa1,
		0xc04d82a5bbd2aa9c,
	}
)

func Test0(t *testing.T)  {
	data := []byte{0, 1, 2}

	for i := 0; i < len(expected); i++ {
		misc.Assert(expected[i] == Siphash(data[:i], tweak))
	}
}
