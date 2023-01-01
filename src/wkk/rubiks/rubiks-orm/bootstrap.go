package rubiks_orm

import (
	"encoding/json"
	"reflect"
	"strconv"
	"time"
	"wkk/common/log"
	"wkk/common/misc"
	"wkk/common/serd"
	"wkk/rubiks/api"
)

var entityIndex = make(map[reflect.Type] map[string][]int)
var entityTable = make(map[reflect.Type]string)

func Register(entity EntityI) {
	rft := reflect.ValueOf(entity).Elem().Type()
	log.Info("register %v", rft)

	if _, ok := entityIndex[rft]; !ok {
		entityIndex[rft] = make(map[string][]int)
	}

	for i := 0; i < rft.NumField(); i += 1 {
		if table, ok := rft.Field(i).Tag.Lookup("primary"); ok {
			entityIndex[rft][table] = append(entityIndex[rft][table], i)

			if existing, ok := entityTable[rft]; ok && existing != table {
				log.Fatal("inconsistent primary index on %v", entity)
			}
			entityTable[rft] = table
		}

		if table, ok := rft.Field(i).Tag.Lookup("index"); ok {
			entityIndex[rft][table] = append(entityIndex[rft][table], i)
		}
	}
}

func encodeRfvForKey(rfv reflect.Value) ([]byte, bool) {
	var result []byte

	switch v := rfv.Interface().(type) {
	case uint64:
		return serd.Append64BE(result, v), true
	case string:
		return append(result, []byte(v)...), true
	case time.Time:
		return serd.Append64BE(result, uint64(v.Second())), true
	}
	return nil, false
}

func getEntityTable(entity EntityI) api.Table {
	rft := reflect.ValueOf(entity).Elem().Type()

	str, ok := entityTable[rft]
	misc.Assert(ok)

	table, err := strconv.ParseUint(str, 10, 64)
	misc.AssertNilError(err)

	return api.Table(table)
}

func primaryIndex(entity EntityI) api.RubiksKK {
	var key []byte

	rfv := reflect.ValueOf(entity).Elem()
	rft := rfv.Type()

	for _, i := range entityIndex[rft][entityTable[rft]] {
		key1, ok1 := encodeRfvForKey(rfv.Field(i))
		misc.Assert(ok1)
		key = append(key, key1...)
	}
	misc.Assert(len(key) > 0)

	return api.RubiksKK{
		Table: getEntityTable(entity),
		Key:   key,
	}
}

func secondaryKK(entity EntityI, index string, pk *api.RubiksKK) api.RubiksKK {
	var key []byte

	rfv := reflect.ValueOf(entity).Elem()
	rft := rfv.Type()

	for _, i := range entityIndex[rft][index] {
		key1, ok1 := encodeRfvForKey(rfv.Field(i))
		misc.Assert(ok1)
		key = append(key, key1...)
	}
	misc.Assert(len(key) > 0)

	table0, err := strconv.ParseUint(index, 10, 64)
	misc.AssertNilError(err)

	if pk != nil {
		pk := primaryIndex(entity)
		key = append(key, pk.Key...)
		key = serd.Append24BE(key, len(pk.Key))
	}

	return api.RubiksKK{
		Table: api.Table(table0),
		Key:   key,
	}
}

func pkInIndex(kk api.RubiksKK) ([]byte, error) {
	key := kk.Key

	subsz, _, err := serd.Get64BE(3, key[len(key)-3:])
	if err != nil {
		return nil, err
	}
	return key[len(key) - 3 - int(subsz):len(key)-3], err
}

func secondaryKKs(entity EntityI, pk *api.RubiksKK) []api.RubiksKK {
	rfv := reflect.ValueOf(entity).Elem()
	var result []api.RubiksKK

	for index, _ := range entityIndex[rfv.Type()] {
		if index != entityTable[rfv.Type()] {
			result = append(result, secondaryKK(entity, index, pk))
		}
	}
	return result
}

func secondaryVVs(entity EntityI) []api.RubiksVV {
	rfv := reflect.ValueOf(entity).Elem()
	var result []api.RubiksVV

	for index, _ := range entityIndex[rfv.Type()] {
		if index != entityTable[rfv.Type()] {
			result = append(result, api.RubiksVV{
				Present: entity.GetPresent(),
				Seqnum:  api.SeqnumInf,	// don't check index seqnum
				Val:     nil,
			})
		}
	}
	return result
}

func commitEntity(entity EntityI) (api.RubiksVV, error) {
	if entity.GetPresent() {
		val, err := json.Marshal(entity)
		if err != nil {
			return api.RubiksVV{}, err
		}
		return api.RubiksVV{
			Present: true,
			Seqnum:  entity.GetSeqnum(),
			Val:     val,
		}, nil
	} else {
		return api.RubiksVV{
			Present: false,
			Seqnum:  entity.GetSeqnum(),
			Val:     nil,
		}, nil
	}
}

func confirmEntity(entity EntityI) (api.RubiksVV, error) {
	return api.RubiksVV{
		Present: entity.GetPresent(),	// doesn't matter
		Seqnum:  entity.GetSeqnum(),
		Val:     nil,
	}, nil
}

func decode(entity EntityI, vv api.RubiksVV) error {
	if vv.Present {
		err := json.Unmarshal(vv.Val, entity)
		if err != nil {
			return err
		}
	}
	entity.SetPresent(vv.Present)
	entity.SetSeqnum(vv.Seqnum)
	return nil
}