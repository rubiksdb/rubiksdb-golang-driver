package api

import (
	"encoding/hex"
	"fmt"
	"wkk/common/serd"
	"wkk/common/unit"
)

const (
	WireMagic = uint16(0x143e)
	PortDelta = 8

	MaxNPairs      = 8
	MaxPairSize    = 15 * unit.KiB
	MaxCommitSize  = 2 * MaxPairSize
	SerializeSize  = 1024 /*stuff*/ + MaxNPairs * MaxPairSize
)

const (
	IterateHintBack   = IterateHint(0x01)

	IterateHintValue  = IterateHint(0x02)
	IterateHintSeqnum = IterateHint(0x04)
	IterateHintAll    = IterateHintValue | IterateHintSeqnum
)

type (
	Seqnum      uint64
	Table       uint64
	Outcome     uint64
	IterateHint uint64
)

const SeqnumInf = ^Seqnum(0)

const (
	OK      = Outcome(0)
	TIMEOUT = Outcome(1)
	INVAL   = Outcome(2)
	STALE   = Outcome(3)
	NONEXT  = Outcome(4)
	EIO     = Outcome(5)
)

const (
	KindGet     = 1
	KindCommit  = 2
	KindConfirm = 3
	KindIterate = 4
)

const (
	TagKind        = 0
	TagNPairs      = 1
	TagPayloadCRC  = 2
	TagOutcome     = 4
	TagPresent     = 5
	TagIterateHint = 6
	TagSeqnum      = 7
)

type RubiksKK struct {
	Table Table
	Key   []byte
}

func (k RubiksKK) String() string {
	return fmt.Sprintf("%d,0x%s", k.Table, hex.EncodeToString(k.Key))
}

type RubiksVV struct {
	Present bool
	Seqnum  Seqnum
	Val     []byte
}

func (v RubiksVV) String() string {
	if v.Present {
		return fmt.Sprintf("present=1,seqnum=%d,0x%s",
			v.Seqnum, hex.EncodeToString(v.Val))
	} else {
		return fmt.Sprintf("present=0,seqnum=%d", v.Seqnum)
	}
}

func SerializeKKS(dst []byte, kks []RubiksKK) []byte {
	mark := dst
	for _, kk := range kks {
		dst = serd.Put64LE(8, dst, uint64(kk.Table))
		dst = serd.Put64LE(3, dst, uint64(len(kk.Key)))
		dst = serd.PutBytes(dst, kk.Key)
	}
	return mark[:len(mark) - len(dst)]
}

func DeserializeKKS(src []byte) ([]RubiksKK, error) {
	var result []RubiksKK

	for len(src) > 0 {
		var table, n uint64
		var err error

		table, src, err = serd.Get64LE(8, src)
		if err != nil {
			return nil, err
		}

		n, src, err = serd.Get64LE(3, src)
		if err != nil {
			return nil, err
		} else if len(src) < int(n) {
			return nil, EIO
		}

		result = append(result, RubiksKK{
			Table: Table(table),
			Key:   src[:n],
		})
		src = src[n:]
	}
	return result, nil
}

func SerializeKVS(dst []byte, kks []RubiksKK, vvs []RubiksVV) []byte {
	mark := dst

	for i, _ := range vvs {
		dst = serd.Put64LE(8, dst, uint64(kks[i].Table))
		dst = serd.Put64LE(3, dst, uint64(len(kks[i].Key)))
		dst = serd.Put64LE(3, dst, uint64(len(vvs[i].Val)))
		dst = serd.PutBytes(dst, kks[i].Key)
		dst = serd.PutBytes(dst, vvs[i].Val)
	}
	return mark[:len(mark)-len(dst)]
}

func DeserializeKVS(src []byte) ([] RubiksKK, []RubiksVV, error) {
	var kks []RubiksKK
	var vvs []RubiksVV

	for len(src) > 0 {
		var table, klen, vlen uint64
		var err error

		table, src, err = serd.Get64LE(8, src)
		if err != nil {
			return nil, nil, err
		}

		klen, src, err = serd.Get64LE(3, src)
		if err != nil {
			return nil, nil, err
		}

		vlen, src, err = serd.Get64LE(3, src)
		if err != nil {
			return nil, nil, err
		}

		if len(src) < int(klen + vlen) {
			return nil, nil, EIO	// bad payload
		}

		kks = append(kks, RubiksKK{
			Table: Table(table),
			Key:   src[:klen],
		})

		vvs = append(vvs, RubiksVV{
			Present: false,	// filled in from nbuf
			Seqnum:  0,		// filled in from nbuf
			Val:     src[klen:klen+vlen],
		})
		src = src[klen+vlen:]
	}
	return kks, vvs, nil
}

func (oc Outcome) Error() string {
	switch oc {
	case TIMEOUT:	return "RUBIKS_TIMEOUT"
	case INVAL:		return "RUBIKS_INVAL"
	case STALE:		return "RUBIKS_STALE"
	case NONEXT:	return "RUBIKS_NONEXT"
	case EIO:		return "RUBIKS_EIO"
	default:		panic("UNREACHABLE")
	}
}

func (oc Outcome) Retryable() bool {
	return oc == EIO
}
