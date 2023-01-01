package client

import (
	"time"
	"wkk/common/log"
	"wkk/common/misc"
	"wkk/network"
	"wkk/rubiks/api"
)

type Rubiks interface {
	RPCGet(rbr *RubiksR, deadline time.Time,
		kks []api.RubiksKK) ([]api.RubiksVV, error)

	RPCCommit(rbr *RubiksR, deadline time.Time,
		kks []api.RubiksKK, vvs []api.RubiksVV) ([]api.RubiksVV, error)

	RPCConfirm(rbr *RubiksR, deadline time.Time,
		kks []api.RubiksKK, vvs []api.RubiksVV) error

	RPCIterate(rbr *RubiksR, deadline time.Time,
		cursor api.RubiksKK, npairs int, hint api.IterateHint) ([]api.RubiksKK, []api.RubiksVV, error)
}

func NewRubiksClient1(epl network.EndpointList, retry Retry) Rubiks {
	return &rubiksClient{
		cm:     NewRubiksCM(epl),
		retry:  retry,
		hintFn: FineHint,
	}
}

func NewRubiksClient(epl network.EndpointList) Rubiks {
	return &rubiksClient{
		cm:     NewRubiksCM(epl),
		retry:  FavoredRetry,
		hintFn: FineHint,
	}
}

type rubiksClient struct {
	cm     *RubiksCM
	retry  Retry
	hintFn func (kk api.RubiksKK)uint64
}

func (client *rubiksClient) RPCGet(rbr *RubiksR, deadline time.Time,
	kks []api.RubiksKK) ([]api.RubiksVV, error) {

	if err := client.retry.Fn(func() error {
		rbr.Begin(deadline)
		rbr.req.MkGET(kks, rbr.payload)
		return client.cm.RPC(rbr, client.hintFn(kks[0]))
	}); err != nil {
		return nil, err
	}

	_, vvs, err := api.DeserializeKVS(rbr.resp.Blob(0).Data)
	if err != nil {
		return nil, err
	}

	present := rbr.resp.Get(api.TagPresent)
	for i := 0; i < len(vvs); i += 1 {
		vvs[i].Seqnum  = rbr.resp.GetSeqnum(i)
		vvs[i].Present = (present & (1 << i)) != 0
	}
	return vvs, err
}

func (client *rubiksClient) RPCCommit(rbr *RubiksR, deadline time.Time,
	kks []api.RubiksKK, vvs []api.RubiksVV) ([]api.RubiksVV, error) {

	if err := client.retry.Fn(func() error {
		rbr.Begin(deadline)
		rbr.req.MkCOMMIT(kks, vvs, rbr.payload)

		if len(rbr.req.Blob(0).Data) > api.MaxCommitSize {
			log.Info("commit size overflow, limit: %d", api.MaxCommitSize)
			// commit size limit
			return api.INVAL
		}

		return client.cm.RPC(rbr, client.hintFn(kks[0]))
	}); err != nil {
		return nil, err
	}

	for i := 0; i < len(vvs); i += 1 {
		vvs[i].Seqnum  = rbr.resp.GetSeqnum(i)
		// fixme check present bit
	}
	return vvs, nil
}

func (client *rubiksClient) RPCConfirm(rbr *RubiksR, deadline time.Time,
	kks []api.RubiksKK, vvs []api.RubiksVV) error {

	return client.retry.Fn(func() error {
		rbr.Begin(deadline)
		rbr.req.MkCONFIRM(kks, vvs, rbr.payload)
		return client.cm.RPC(rbr, client.hintFn(kks[0]))
	})
}

func (client *rubiksClient) RPCIterate(rbr *RubiksR, deadline time.Time,
	cursor api.RubiksKK, npairs int, hint api.IterateHint) ([]api.RubiksKK, []api.RubiksVV, error) {

	if err := client.retry.Fn(func() error {
		rbr.Begin(deadline)
		rbr.req.MkITERATE(cursor, hint, npairs, rbr.payload)
		return client.cm.RPC(rbr, client.hintFn(cursor))
	}); err != nil {
		return nil, nil, err
	}

	// not necessary equals to the desired npairs, but can't be zero
	npairs = int(rbr.resp.Get(api.TagNPairs))
	misc.Assert(npairs != 0)

	if hint & api.IterateHintValue == api.IterateHintValue {
		kks, vvs, err := api.DeserializeKVS(rbr.resp.Blob(0).Data)
		if err != nil {
			return nil, nil, err
		}
		if len(kks) != len(vvs) {
			return nil, nil, api.EIO
		}
		if len(kks) != npairs {
			return nil, nil, err
		}

		for i := 0; i < len(vvs); i += 1 {
			vvs[i].Present = true

			if hint & api.IterateHintSeqnum == api.IterateHintSeqnum {
				vvs[i].Seqnum = rbr.resp.GetSeqnum(i)
			}
		}
		return kks, vvs, nil
	} else {
		kks, err := api.DeserializeKKS(rbr.resp.Blob(0).Data)
		if err != nil {
			return nil, nil, err
		}
		if len(kks) != npairs {
			return nil, nil, api.EIO
		}
		return kks, nil, nil
	}
}