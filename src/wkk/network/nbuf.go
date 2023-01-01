package network

import (
	"wkk/common/misc"
	"wkk/common/serd"
)

type Nbuf struct {
	mask uint64
	payload  [63]uint64
}

func (t *Nbuf) Reset()  {
	t.mask = 0
}

func (t *Nbuf) HackMask() uint64 {
	return t.mask
}

func (t *Nbuf) Has(tag uint64) bool {
	misc.Assert(tag < 63)
	return t.mask & (1 << tag) == 1 << tag
}

func (t *Nbuf) Have(mask uint64) bool {
	return t.mask & mask == mask
}

func (t *Nbuf) Get(tag uint64) uint64 {
	misc.Assert(t.Has(tag))
	return t.payload[tag]
}

func (t *Nbuf) GetDefault(tag, payload uint64) uint64 {
	if t.Has(tag) {
		return t.payload[tag]
	} else {
		return payload
	}
}

func (t *Nbuf) Put(tag, payload uint64)  {
	misc.Assert(!t.Has(tag))
	t.payload[tag] = payload
	t.mask |= 1 << tag
}

func (t *Nbuf) Serialize(dst []byte) []byte {
	dst = serd.Put64LE(8, dst, t.mask)

	for i := 0; i < 63; i += 1 {
		if t.mask & (1 << i) == (1 << i) {
			dst = serd.Put64LE(8, dst, t.payload[i])
		}
	}
	return dst
}

func (t *Nbuf) Deserialize(src []byte) ([]byte, error) {
	var err error

	t.mask, src, err = serd.Get64LE(8, src)
	if err != nil {
		return nil, err
	}

	for i := 0; i < 63; i += 1 {
		if t.mask & (1 << i) == (1 << i) {
			t.payload[i], src, err = serd.Get64LE(8, src)
			if err != nil {
				return nil, err
			}
		}
	}
	return src, nil
}