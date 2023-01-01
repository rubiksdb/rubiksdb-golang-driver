package client

import (
	"time"
	"wkk/common/misc"
	"wkk/host/api"
	"wkk/network"
)

type HostCM struct {
    net network.CM
}

func NewHostCM() *HostCM {
    return &HostCM{
        net: network.NewCM("host", api.Timeout, api.WireMagic, api.SerializeSize),
    }
}

func (hcm *HostCM) RPC(hr *HostR, ep network.Endpoint) error {
    if err := hcm.net.RPC(hr, ep); err != nil {
        return err
    }

    if oc := api.Outcome(hr.resp.Get(api.TagOutcome)); oc != api.OK {
        return oc
    }
    return nil
}

func NewHostR() *HostR {
    return &HostR{
        requestId: 0,
        wakeup:    make(chan struct{}, 1),
        serialize: make([]byte, api.SerializeSize),
    }
}

/****** host request ******/
type HostR struct {
    req  api.HostMessage
    resp api.HostMessage

    requestId uint64
    deadline  time.Time

    wakeup    chan struct{}
    serialize []byte
}

func (r *HostR) Deadline() time.Time {
    return r.deadline
}

func (r *HostR) Wakeup() chan struct{} {
    return r.wakeup
}

func (r *HostR) RequestId() uint64 {
    return r.requestId
}

func (r *HostR) Serialize(requestId, clientId uint64) []byte {
    r.req.PutHdr(r.deadline, requestId, clientId)
    r.requestId = requestId // mark the requestId

    return r.req.Serialize(r.serialize)
}

func (r *HostR) Deserialize(src[] byte) error {
    n := copy(r.serialize, src)
    misc.Assert(n == len(src))

    return r.resp.Deserialize(r.serialize[:n])
}

func (r *HostR) CreateFirstReplica(deadline time.Time,
    clusterId uint64,
    extentType uint64,
    extentId uint64,
    nominal network.Endpoint)  {
    r.requestId = 0
    r.deadline  = deadline

    r.req.CreateFirstReplica(clusterId, extentType, extentId, nominal)
}

func (r *HostR) CreateExtraReplica(deadline time.Time,
    clusterId, extentType, extentId uint64,
    ecrow, participantId uint64) {
    r.requestId = 0
    r.deadline  = deadline

    r.req.CreateExtraReplica(clusterId, extentType, extentId, ecrow, participantId)
}