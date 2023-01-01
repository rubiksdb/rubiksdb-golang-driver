package rubiks_orm

import (
	"reflect"
	"time"
	"wkk/rubiks/api"
	"wkk/rubiks/client"
)

type EntityI interface {
	GetPresent() bool
	SetPresent(present bool)

	GetSeqnum() api.Seqnum
	SetSeqnum(seqnum api.Seqnum)
}

type EntityBase struct {
	present bool
	seqnum  api.Seqnum
}

func (e *EntityBase) GetPresent() bool {
	return e.present
}

func (e *EntityBase) SetPresent(present bool)  {
	e.present = present
}

func (e *EntityBase) GetSeqnum() api.Seqnum {
	return e.seqnum
}

func (e *EntityBase) SetSeqnum(seqnum api.Seqnum)  {
	e.seqnum = seqnum
}

func deadline() time.Time {
	return time.Now().Add(1 * time.Second)
}

// interface
type RubiksOrm interface {
	Get(entities ...EntityI) error

	Confirm(entities ...EntityI) error

	Commit(entities ...EntityI) error

	ListBy(entity EntityI, index string) (chan EntityI, chan error)
}

func NewRubiksOrm(rubiks client.Rubiks) RubiksOrm {
	return &rubiksOrm{
		rbr:    client.NewRubiksR(),
		rbr2:   client.NewRubiksR(),
		rubiks: rubiks,
	}
}

// implementation
type rubiksOrm struct {
	// thread unsafe
	rbr    *client.RubiksR
	rbr2   *client.RubiksR	// for iterate and get

	rubiks client.Rubiks
}

func (orm *rubiksOrm) Get(entities...EntityI) error {
	var kks []api.RubiksKK

	for _, ent := range entities {
		kks = append(kks, primaryIndex(ent))
	}

	vvs, err := orm.rubiks.RPCGet(orm.rbr, deadline(), kks)
	if err != nil {
		return err
	}
	for i, _ := range vvs {
		err := decode(entities[i], vvs[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (orm *rubiksOrm) Confirm(entities...EntityI) error {
	var kks []api.RubiksKK
	var vvs []api.RubiksVV

	for _, ent := range entities {
		kks = append(kks, primaryIndex(ent))

		vv, err := confirmEntity(ent)
		if err != nil {
			return err
		}
		vvs = append(vvs, vv)
	}

	return orm.rubiks.RPCConfirm(orm.rbr, deadline(), kks, vvs)
}

func (orm *rubiksOrm) Commit(entities...EntityI) error {
	var kks []api.RubiksKK
	var vvs []api.RubiksVV

	// primary kk/vv
	for _, ent := range entities {
		kks = append(kks, primaryIndex(ent))

		vv, err := commitEntity(ent)
		if err != nil {
			return err
		}
		vvs = append(vvs, vv)
	}

	// collect index kk/vv
	for k, ent := range entities {
		kks = append(kks, secondaryKKs(ent, &kks[k])...)
		vvs = append(vvs, secondaryVVs(ent)...)
	}

	vvs, err := orm.rubiks.RPCCommit(orm.rbr, deadline(), kks, vvs)
	if err != nil {
		return err
	}

	if len(vvs) != len(kks) {
		return api.EIO
	}

	for i, ent := range entities {
		ent.SetSeqnum(vvs[i].Seqnum)
	}
	return nil
}

func (orm *rubiksOrm) ListBy(entity EntityI, index string) (chan EntityI, chan error) {
	rc, ec := make(chan EntityI), make(chan error)
	cursor := secondaryKK(entity, index, nil /*=!pk*/)

	go func() {
		defer close(rc)
		defer close(ec)

		primaryTable := getEntityTable(entity)

		// ptr
		rft := reflect.ValueOf(entity).Elem().Type()

		for {
			var primaryKKs []api.RubiksKK

			kks, vvs, err := orm.rubiks.RPCIterate(orm.rbr,
				deadline(), cursor, api.MaxNPairs, 0 /*!hint*/)
			if err == api.NONEXT {
				break
			}

			if err != nil {
				ec <- err
				return
			}

			// convert index to primary key
			for _, kk := range kks {
				pk, err := pkInIndex(kk)
				if err != nil {
					ec <- err
					return
				}

				primaryKKs = append(primaryKKs, api.RubiksKK{
					Table: primaryTable,
					Key:   pk,
				})
			}

			if len(primaryKKs) == 0 {
				break
			}

			vvs, err = orm.rubiks.RPCGet(orm.rbr2, deadline(), primaryKKs)
			if err != nil {
				ec <- err
				return
			}

			for _, vv := range vvs {
				if vv.Present { // may deleted after RPCIterate
					entity := reflect.New(rft).Interface().(EntityI)
					err := decode(entity, vv)
					if err != nil {
						ec <- err
						break
					}
					rc <- entity
				}
			}

			cursor = kks[len(primaryKKs) - 1]
		}
	}()

	return rc, ec
}