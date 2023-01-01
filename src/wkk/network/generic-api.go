package network

const (
	TagClientID      = 0x00
	TagRequestID     = 0x01
	TagAllowance     = 0x02
	TagServiceTime = 0x03
	TagECN         = 0x04

	KindBitResponse = 0x100
)

func Bit(tag uint64) uint64 {
	return 1 << tag
}