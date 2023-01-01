package blob

import "wkk/common/crc128"

type T struct {
    Data []byte
    CRC  crc128.T
}

func Seal(data []byte, crc0 crc128.T) T {
    return T{
        Data: data,
        CRC:  crc128.Update(crc0, data),
    }
}

func OK(t T, crc0 crc128.T) bool {
    return crc128.Eq(crc128.Update(crc0, t.Data), t.CRC)
}