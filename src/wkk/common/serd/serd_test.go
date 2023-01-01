package serd

import (
	"testing"
	"wkk/common/misc"
)

func Test0(t *testing.T)  {
	data := []byte{0x31, 0x0E, 0x0E, 0xDD, 0x47, 0xDB, 0x6F, 0x72}

	actual1, _, _ := Get64LE(1, data[:])
	actual3, _, _ := Get64LE(3, data[:])
	actual8, _, _ := Get64LE(8, data[:])

	misc.Assert(uint64(0x31) == actual1)
	misc.Assert(uint64(0x0E0E31) == actual3)
	misc.Assert(uint64(0x726FDB47DD0E0E31) == actual8)
}

func Test1(t *testing.T)  {
	data := []byte{0x31, 0x0E, 0x0E, 0xDD, 0x47, 0xDB, 0x6F, 0x72}

	actual1, _, _ := Get64BE(1, data[:])
	actual3, _, _ := Get64BE(3, data[:])
	actual8, _, _ := Get64BE(8, data[:])

	misc.Assert(uint64(0x31) == actual1)
	misc.Assert(uint64(0x310E0E) == actual3)
	misc.Assert(uint64(0x310E0EDD47DB6F72) == actual8)
}