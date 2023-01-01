package serd

import (
	"io"
	"wkk/common/misc"
)

func Get64LE(n int, src []byte) (uint64, []byte, error) {
	if len(src) < n {
		return 0, nil, io.EOF
	}

	result := uint64(0)
	for i := 0; i < n; i += 1 {
		result |= uint64(src[i]) << (i * 8)
	}
	return result, src[n:], nil
}

func Put64LE(n int, dst []byte, val uint64) []byte {
	misc.Assert(len(dst) >= n)

	for i := 0; i < n; i += 1 {
		dst[i] = uint8(val >> (i * 8))
	}
	return dst[n:]
}

func Get64BE(n int, src []byte) (uint64, []byte, error) {
	if len(src) < 0 {
		return 0, nil, io.EOF
	}

	result := uint64(0)
	for i := 0; i < n; i += 1 {
		result = (result << 8) | uint64(src[i])
	}
	return result, src[n:], nil
}

func Put64BE(n int, dst []byte, val uint64) []byte {
	misc.Assert(len(dst) >= n)

	for i := 0; i < n; i += 1 {
		dst[n-i-1] = uint8(val >> (i * 8))
	}
	return dst[n:]
}

func Append64BE(dst []byte, val uint64) []byte {
	return append(dst,
		byte(val >> 56),
		byte(val >> 48),
		byte(val >> 40),
		byte(val >> 32),
		byte(val >> 24),
		byte(val >> 16),
		byte(val >> 8),
		byte(val))
}

func Append24BE(dst []byte, val int) []byte {
	return append(dst,
		byte(val >> 16),
		byte(val >> 8),
		byte(val))
}

func PutBytes(dst []byte, src []byte) []byte {
	misc.Assert(len(dst) >= len(src))
	copy(dst, src)
	return dst[len(src):]
}
