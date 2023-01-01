package crc128

import (
	"encoding/binary"
	"fmt"
)

// with polynomial P = x^128 + x^7 + x^2 + x + 1

type T struct {
	V [2]uint64
}

func MkCRC(v0, v1 uint64) T {
	return T{V: [2]uint64{v0, v1}}
}

func Update(t T, data []byte) T {
	for len(data) > 7 {
		t = update0(t, binary.LittleEndian.Uint64(data), 64)
		data = data[8:]
	}

	for len(data) > 0 {
		t = update0(t, uint64(data[0]), 8)
		data = data[1:]
	}
	return t
}

func update0(t T, payload uint64, n uint64) T {
	result := T{V: [2]uint64{t.V[0], t.V[1] ^ payload}}
	tmp := shl(result, 128-n)

	result = shr(result, n)
	result = xor(result, tmp)
	result = xor(result, shr(tmp, 1))
	result = xor(result, shr(tmp, 2))
	result = xor(result, shr(tmp, 7))
	return result
}

func shl(t T, n uint64) T {
	if n > 127 {
		return T{V: [2]uint64{0, 0}}
	} else if n > 63 {
		return T{V: [2]uint64{0, t.V[0] << (n - 64)}}
	} else {
		return T{V: [2]uint64{t.V[0] << n, t.V[1] << n | t.V[0] >> (64-n)}}
	}
}

func shr(t T, n uint64) T {
	if n > 127 {
		return T{V: [2]uint64{0, 0}}
	} else if n > 63 {
		return T{V: [2]uint64{t.V[1] >> (n-64), 0}}
	} else {
		return T{V: [2]uint64{t.V[0] >> n | t.V[1] << (64-n), t.V[1] >> n}}
	}
}

func xor(t0, t1 T) T {
	return T{V: [2]uint64{t0.V[0] ^ t1.V[0], t0.V[1] ^ t1.V[1]}}
}

func (t T) String() string {
	return fmt.Sprintf("CRC128(0x%x,0x%x", t.V[0], t.V[1])
}

func Eq(t0, t1 T) bool {
	return t0.V[0] == t1.V[0] && t0.V[1] == t1.V[1]
}