package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"wkk/common/perm"
	"wkk/common/serd"
)

const (
	DataUint  = Kind(1)
	DataSmall = Kind(2)
	DataBig   = Kind(3)
	DataMix   = Kind(4)
)

type Kind int

func (k Kind) String() string {
	switch k {
	case DataUint:  return "uint"
	case DataSmall:	return "small"
	case DataBig:   return "big"
	case DataMix:	return "mix"
	default:		return "unknown"
	}
}

var SMALL = [][]byte {
	bytes.Repeat([]byte("hello-world "), 2),
	bytes.Repeat([]byte("yummy-bear "), 2),
	bytes.Repeat([]byte("trader-joe "), 2),
	bytes.Repeat([]byte("key-value-db "), 2),
	bytes.Repeat([]byte("wakaka-go-go "), 2),
	bytes.Repeat([]byte("rubiks-fun "), 2),
	bytes.Repeat([]byte("222222 "), 2),
	bytes.Repeat([]byte("3333333333 "), 2),
}

var BIG = [][]byte {
	bytes.Repeat([]byte("hello-world "), 40),
	bytes.Repeat([]byte("yummy-bear "), 40),
	bytes.Repeat([]byte("trader-joe "), 40),
	bytes.Repeat([]byte("brand-new "), 40),
	bytes.Repeat([]byte("wakaka-go-go "), 40),
	bytes.Repeat([]byte("rubiks-run "), 40),
	bytes.Repeat([]byte("222222 "), 40),
	bytes.Repeat([]byte("3333333333 "), 40),
}

type pair struct {
	K uint64
	V []byte
}

type DataSet struct {
	Lo, Hi uint64
	Perm   bool
	Kind   Kind
}

func (d *DataSet) Set(s string) error {
	var err error
	offset, count, permute, kind := uint64(0), uint64(1000), false, DataUint

	for _, ss := range strings.Split(s, ",") {
		sss := strings.Split(ss, "=")

		if len(sss) == 2 {
			switch sss[0] {
			case "o", "off", "offset":
				offset, err = strconv.ParseUint(sss[1], 10, 64)
				if err != nil {
					return err
				}
			case "c", "count":
				count, err = strconv.ParseUint(sss[1], 10, 64)
				if err != nil {
					return err
				}
			}
		} else {
			switch ss {
			case "uint":		kind = DataUint
			case "s", "small":	kind = DataSmall
			case "b", "big":    kind = DataBig
			case "m", "mix":	kind = DataMix
			case "perm":		permute = true
			}
		}
	}

	d.Lo = offset
	d.Hi = offset + count
	d.Perm = permute
	d.Kind = kind
	return nil
}

func (d DataSet) String() string {
	if d.Perm {
		return fmt.Sprintf("offset=%d,count=%d,%d,perm", d.Lo, d.Hi-d.Lo, d.Kind)
	} else {
		return fmt.Sprintf("offset=%d,count=%d,%s", d.Lo, d.Hi-d.Lo, d.Kind)
	}
}

func (d DataSet) Split(i, n int) DataSet {
	m := (d.Hi - d.Lo) / uint64(n)
	return DataSet{
		Lo:   d.Lo + m * uint64(i),
		Hi:   d.Lo + m * uint64(i+1),
		Perm: d.Perm,
		Kind: d.Kind,
	}
}

func (d *DataSet) GetK() chan uint64{
	ch := make(chan uint64)

	go func() {
		defer close(ch)

		for i := d.Lo; i < d.Hi; i += 1 {
			ch <- i
		}
	}()
	return ch
}

func (d *DataSet) permMaybe(i uint64) uint64 {
	if d.Perm {
		return perm.Perm64(i)
	}
	return i
}

func (d *DataSet) GetKV() chan pair {
	ch := make(chan pair)

	go func() {
		var data [8]byte
		defer close(ch)

		switch d.Kind {
		case DataUint:
			for i := d.Lo; i < d.Hi; i += 1 {
				serd.Append64BE(data[:], i)
				ch <- pair{K: d.permMaybe(i), V: data[:8]}
			}

		case DataSmall:
			for i := d.Lo; i < d.Hi; i += 1 {
				ch <- pair{K: d.permMaybe(i), V: SMALL[i%8][:]}
			}

		case DataBig:
			for i := d.Lo; i < d.Hi; i += 1 {
				ch <- pair{K: d.permMaybe(i), V: BIG[i%8][:]}
			}

		case DataMix:
			for i := d.Lo; i < d.Hi; i += 1 {
				if i % 2 == 0 {
					ch <- pair{K: d.permMaybe(i), V: SMALL[i%8][:]}
				} else {
					ch <- pair{K: d.permMaybe(i), V: BIG[i%8][:]}
				}
			}
		}
	}()
	return ch
}
