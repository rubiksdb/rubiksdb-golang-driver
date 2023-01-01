package client

import (
	"time"
	"wkk/common/misc"
	"wkk/common/perm"
	"wkk/common/siphash"
	"wkk/network"
	"wkk/rubiks/api"
)

const RevivePeriod = time.Minute

type RubiksCM struct {
	gcm  network.CM
	epl  network.EndpointList
	sick []time.Time
}

func NewRubiksCM(epl network.EndpointList) *RubiksCM {
	cm := &RubiksCM{
		gcm:  network.NewCM("rubiks", api.TIMEOUT, api.WireMagic, api.SerializeSize),
		epl:  epl,
		sick: make([]time.Time, len(epl)),
	}
	for i, _ := range cm.sick {
		cm.sick[i] = time.Time{}
	}
	return cm
}

func FineHint(kk api.RubiksKK) uint64 {
	sz := len(kk.Key)

	if sz > 1 {
		return perm.Perm64(uint64(kk.Table)) ^
			siphash.Siphash(kk.Key[:sz-1], siphash.DefaultTweak)
	} else {
		return perm.Perm64(uint64(kk.Table))
	}
}

func CoarseHint(kk api.RubiksKK) uint64 {
	sz := len(kk.Key)

	if sz > 2 {
		return perm.Perm64(uint64(kk.Table)) ^
			siphash.Siphash(kk.Key[:sz-2], siphash.DefaultTweak)
	} else {
		return perm.Perm64(uint64(kk.Table))
	}
}

func (cm *RubiksCM) Submit(rbr *RubiksR, hint uint64) error {
	victim, err := cm.pick(hint)
	if err != nil {
		return err
	}

	err = cm.gcm.Submit(rbr, cm.epl[victim])
	if err != nil {
		cm.sick[victim] = time.Now()
		return api.EIO
	}
	return nil
}

func (cm *RubiksCM) WaitForCompletion(rbr *RubiksR) error {
	err := cm.gcm.WaitForCompletion(rbr)
	if err != nil {
		return err
	}

	oc := api.Outcome(rbr.resp.Get(api.TagOutcome))
	if oc != api.OK {
		return oc
	}
	return nil
}

func (cm *RubiksCM) RPC(rbr *RubiksR, hint uint64) error {
	err := cm.Submit(rbr, hint)
	if err != nil {
		return err
	}
	return cm.WaitForCompletion(rbr)
}

func (cm *RubiksCM) pick(hint uint64) (int, error) {
	victim, max, now := -1, uint64(0), time.Now()

reviveAndRetry:
	for i, ep := range cm.epl {
		if now.After(cm.sick[i].Add(RevivePeriod)) {
			if tmp := ep.U64() ^ hint; tmp >= max {
				victim, max = i, tmp
			}
		}
	}

	if victim == -1 {
		// no available candidate, revive all
		for i := range cm.sick {
			cm.sick[i] = time.Time{}
		}
		goto reviveAndRetry
	}
	return victim, nil
}

type RubiksR struct {
	req  api.RubiksMessage
	resp api.RubiksMessage

	// CM
	requestId uint64
	deadline  time.Time

	wakeup    chan struct{}
	serialize []byte
	payload   []byte
}

func NewRubiksR() *RubiksR {
	return &RubiksR{
		wakeup:    make(chan struct{}, 1),
		serialize: make([]byte, api.SerializeSize),
		payload:   make([]byte, api.MaxNPairs * (3 + api.MaxPairSize)),
	}
}

func (r *RubiksR) Begin(deadline time.Time)  {
	r.requestId = 0
	r.deadline  = deadline

	misc.Poison(r.payload, true)
	misc.Poison(r.serialize, true)
}

func (r *RubiksR) Deadline() time.Time {
	return r.deadline
}

func (r *RubiksR) Wakeup() chan struct{} {
	return r.wakeup
}

func (r *RubiksR) RequestId() uint64 {
	return r.requestId
}

func (r *RubiksR) Serialize(requestId, clientId uint64) []byte {
	r.requestId = requestId	// mark requestId
	r.req.PutHdr(r.deadline, requestId, clientId)

	return r.req.Serialize(r.serialize)
}

func (r *RubiksR) Deserialize(src []byte) error {
	n := copy(r.serialize, src)
	misc.Assert(n == len(src))

	return r.resp.Deserialize(r.serialize[:n])
}
