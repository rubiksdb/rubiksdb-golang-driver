package api

import (
	"time"
	"wkk/common/blob"
	"wkk/common/crc128"
	"wkk/common/misc"
	"wkk/network"
)

const (
	PayloadCRC_0 = 0xe8a8918ad6ebdce4
	PayloadCRC_1 = 0x60457c9dceee5eff
)

type RubiksMessage struct {
	hdr     network.Nbuf
	msg     network.Nbuf
	payload blob.T
}

var PayloadZero = blob.T{
	Data: nil,
	CRC:  crc128.MkCRC(PayloadCRC_0, PayloadCRC_1),
}

func (t *RubiksMessage) PutHdr(deadline time.Time, requestId, clientId uint64)  {
	allowance := deadline.Sub(time.Now()).Microseconds()
	if allowance < 0 {
		allowance = 0
	}

	t.hdr.Put(network.TagClientID, clientId)
	t.hdr.Put(network.TagRequestID, requestId)
	t.hdr.Put(network.TagAllowance, uint64(allowance))
}

func (t *RubiksMessage) Hdr(tag uint64) uint64 {
	return t.hdr.Get(tag)
}

func (t *RubiksMessage) putCRC(tag uint64, crc crc128.T)  {
	t.Put(tag + 0, crc.V[0])
	t.Put(tag + 1, crc.V[1])
}

func (t *RubiksMessage) GetCRC(tag uint64) crc128.T {
	return crc128.T{
		V: [2]uint64{t.Get(tag + 0), t.Get(tag + 1)},
	}
}

func (t *RubiksMessage) Serialize(dst []byte) []byte {
	if len(t.payload.Data) > 0 {
		t.putCRC(TagPayloadCRC, t.payload.CRC)
	}

	nbufs := []*network.Nbuf{&t.hdr, &t.msg}
	blobs := [][]byte{t.payload.Data}
	return network.Serialize(dst, WireMagic, nbufs, blobs)
}

func (t *RubiksMessage) Deserialize(src []byte) error {
	nbufs := []*network.Nbuf{&t.hdr, &t.msg}
	blobs := [][]byte{nil}

	if err := network.Deserialize(src, nbufs, blobs); err != nil {
		return err
	}

	t.payload = PayloadZero

	if len(blobs[0]) > 0 {
		t.payload = blob.T{Data: blobs[0], CRC: t.GetCRC(TagPayloadCRC)}

		if !blob.OK(t.payload, PayloadZero.CRC) {
			return EIO
		}
	}
	return nil
}

func (t *RubiksMessage) Reset(kind uint64, npairs int, payload blob.T)  {
	t.hdr.Reset()
	t.msg.Reset()

	t.Put(TagKind, kind)
	t.Put(TagNPairs, uint64(npairs))
	t.payload = payload
}

func (t *RubiksMessage) Blob(i int) blob.T {
	switch i {
	case 0:		return t.payload
	default:	panic("UNREACHABLE")
	}
}

func (t *RubiksMessage) Put(tag, payload uint64) {
	t.msg.Put(tag, payload)
}

func (t *RubiksMessage) Get(tag uint64) uint64 {
	return t.msg.Get(tag)
}

func (t *RubiksMessage) GetSeqnum(i int) Seqnum {
	return Seqnum(t.Get(TagSeqnum + (uint64(i))))
}

func (t *RubiksMessage) MkGET(kks []RubiksKK, serializeKKs []byte)  {
	key := blob.Seal(SerializeKKS(serializeKKs, kks), PayloadZero.CRC)

	t.Reset(KindGet, len(kks), key)
}

func (t *RubiksMessage) MkCOMMIT(kks []RubiksKK, vvs []RubiksVV, serializeKKs []byte)  {
	misc.Assert(len(kks) == len(vvs))

	t.Reset(KindCommit, len(kks),
		blob.Seal(SerializeKVS(serializeKKs, kks, vvs), PayloadZero.CRC))

	present := uint64(0)
	for i := 0; i < len(vvs); i += 1 {
		t.Put(TagSeqnum + uint64(i), uint64(vvs[i].Seqnum))

		if vvs[i].Present {
			present |= 1 << i
		}
	}
	t.Put(TagPresent, present)
}

func (t *RubiksMessage) MkCONFIRM(kks []RubiksKK, vvs []RubiksVV, serializeKKs []byte)  {
	misc.Assert(len(kks) == len(vvs))

	t.Reset(KindConfirm, len(kks),
		blob.Seal(SerializeKKS(serializeKKs, kks), PayloadZero.CRC))

	for i := 0; i < len(vvs); i += 1 {
		t.Put(TagSeqnum + uint64(i), uint64(vvs[i].Seqnum))
	}
}

func (t *RubiksMessage) MkITERATE(kk RubiksKK, hint IterateHint, npairs int, serializeKKs []byte) {
	t.Reset(KindIterate, npairs,
		blob.Seal(SerializeKKS(serializeKKs, []RubiksKK{kk}), PayloadZero.CRC))
	t.Put(TagIterateHint, uint64(hint))
}