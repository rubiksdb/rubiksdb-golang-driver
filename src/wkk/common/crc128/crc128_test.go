package crc128

import (
	"testing"
	"wkk/common/misc"
)

func Test0(t *testing.T)  {
	data := []byte{
		0x00, 0xba, 0xf0, 0x01, 0xec, 0xcf, 0xdb, 0x1d, 0x95, 0xba,
		0x06, 0x09, 0x58, 0xad, 0x79, 0x9a, 0x31, 0x27, 0xb3, 0x49, 0x9b,
		0x7b, 0xfd, 0xf5, 0x8d, 0x75, 0xe5, 0xbb, 0x71, 0xea, 0x6e, 0x37,
		0x3a, 0x96, 0x7c, 0xc5, 0xf6, 0x0b, 0x8c, 0x26, 0xd2, 0x5f, 0x06,
		0xce, 0x64, 0x16, 0xe9, 0x53, 0x06, 0x65, 0xe9, 0x38, 0x39, 0xde}

	crc0 := T{V: [2]uint64{0xa668877af48a11c8, 0xf566d9ef7e137f5f}}
    expect := T{V: [2]uint64{0xd736f6aefee102e7, 0x7be4e7fe782ce3d9}}

	misc.Assert(Eq(Update(crc0, data), expect))
}
