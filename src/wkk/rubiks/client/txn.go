package client

import (
    "time"
	"wkk/common/misc"
	"wkk/rubiks/api"
)

type Txn struct {
    rubiks Rubiks

    rbr *RubiksR
    kks []api.RubiksKK
    vvs []api.RubiksVV
}

func BeginTxn(rubiks Rubiks, rbr *RubiksR) *Txn {
    return &Txn{
        rubiks: rubiks,
        rbr:    rbr,
    }
}

func (t *Txn) Put(kk api.RubiksKK, vv api.RubiksVV) {
    misc.Assert(len(t.kks) < api.MaxNPairs)

    t.kks = append(t.kks, kk)
    t.vvs = append(t.vvs, vv)
}

func (t *Txn) Confirm() error {
    if len(t.kks) == 0 {
        return nil
    }
    deadline := time.Now().Add(time.Second)

    return t.rubiks.RPCConfirm(t.rbr, deadline, t.kks, t.vvs)
}

func (t *Txn) Commit() error {
    if len(t.kks) == 0 {
        return nil
    }
    deadline := time.Now().Add(time.Second)

    // fixme -- return the updated seqnum
    _, err := t.rubiks.RPCCommit(t.rbr, deadline, t.kks, t.vvs)
    return err
}