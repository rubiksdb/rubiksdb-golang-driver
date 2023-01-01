package api

import (
	"errors"
	"time"
	"wkk/common/blob"
	"wkk/common/misc"
	"wkk/network"
)

type HostMessage struct {
	hdr network.Nbuf
	msg network.Nbuf
}

func (t *HostMessage) PutHdr(deadline time.Time, requestId, clientId uint64)  {
	// remove 1 millis for RTT
	allowance := (deadline.Sub(time.Now()) - time.Millisecond).Microseconds()
	if allowance < 0 {
		allowance = 0
	}

	t.hdr.Put(network.TagClientID, clientId)
	t.hdr.Put(network.TagRequestID, requestId)
	t.hdr.Put(network.TagAllowance, uint64(allowance))
}

func (t *HostMessage) Serialize(dst []byte) []byte {
	return network.Serialize(dst,
		WireMagic, []*network.Nbuf{&t.hdr, &t.msg}, [][]byte{})
}

func (t *HostMessage) Deserialize(src []byte) error {
	nbufs := []*network.Nbuf{&t.hdr, &t.msg}
	blobs := [][]byte{nil}

	if err := network.Deserialize(src, nbufs, blobs); err != nil {
		return err
	} else if len(blobs[0]) != 0 {
		return errors.New("bad message")
	}
	return nil
}

func (t *HostMessage) Reset(kind uint64)  {
	t.hdr.Reset()
	t.msg.Reset()

	t.msg.Put(TagKind, kind)
}

func (t *HostMessage) Blob(i int) blob.T {
	misc.Assert(false) // unreachable
	return blob.T{}
}

func (t *HostMessage) Put(tag, payload uint64)  {
	t.msg.Put(tag, payload)
}

func (t *HostMessage) Get(tag uint64) uint64 {
	return t.msg.Get(tag)
}

func (t *HostMessage) CreateFirstReplica(
	clusterId, extentType, extentId uint64, ep network.Endpoint)  {
	t.Reset(KindCreateFirstReplica)
	t.Put(TagClusterID, clusterId)
	t.Put(TagExtentType, uint64(extentType))
	t.Put(TagExtentID, uint64(extentId))
	t.Put(TagParticipantAddr, uint64(ep.IpU32()))
	t.Put(TagParticipantPort, uint64(ep.Port))
}

func (t *HostMessage) CreateExtraReplica(
	clusterId, extentType, extentId uint64,
	ecrow, participantId uint64)  {
	t.Reset(KindCreateExtraReplica)
	t.Put(TagClusterID, clusterId)
	t.Put(TagExtentType, uint64(extentType))
	t.Put(TagExtentID, uint64(extentId))
	t.Put(TagEcrow, ecrow)
	t.Put(TagParticipantID, participantId)
}