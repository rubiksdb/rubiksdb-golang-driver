package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
	"wkk/common/log"
	"wkk/common/serd"
	"wkk/network"
	"wkk/rubiks/api"
	"wkk/rubiks/client"
)

func deadline() time.Time {
	return time.Now().Add(time.Second)
}

func main() {
	epl, rr, wr, batch, ds := parseInput()

	rubiks := client.NewRubiksClient1(epl, client.FavoredRetry)
	prog := make(chan string)

	log.Info("rubiks performance test with: %s", ds)

	var group sync.WaitGroup
	group.Add(rr + wr)

	for i := 0; i < rr; i += 1 {
		go readRoutine(rubiks, batch, ds.Split(i, rr), prog, &group)
	}

	for i := 0; i < wr; i += 1 {
		go writeRoutine(rubiks, batch, ds.Split(i, wr), prog, &group)
	}

	// log progress
	go func() {
		for one := range prog {
			log.Info("%s", one)
		}
	}()

	group.Wait()
	close(prog)
	log.Info("all done!!")
}

func readRoutine(rubiks client.Rubiks, batch int, s DataSet,
	prog chan string, group *sync.WaitGroup)  {
	var keys []uint64

	space := make([]byte, 1024)
	rbr := client.NewRubiksR()
	lat := NewLatency()
	defer group.Done()

	cntPresent, cntTotal, cntError := 0, 0, 0
	t0 := time.Now()

	for k := range s.GetK() {
		keys = append(keys, k)

		if len(keys) == batch {
			kks, t0 := mkKKs(keys, space), time.Now()

			vvs, err := rubiks.RPCGet(rbr, deadline(), kks)
			if err != nil {
				prog <- fmt.Sprintf("read: %s", err)
				cntError += 1
			} else {
				lat.Measure(t0)

				if lat.count == 100 {
					prog <- fmt.Sprintf("read batch %d: %s", batch, lat)
					lat.Reset()
				}
			}

			cntTotal += len(kks)
			for _, vv := range vvs {
				if vv.Present {
					cntPresent += 1
				}
			}

			keys = nil
		}
	}

	prog <- fmt.Sprintf("read routine -- present %d/%d, error %d, duration %s",
		cntPresent, cntTotal, cntError, time.Now().Sub(t0))
}

func writeRoutine(rubiks client.Rubiks, batch int, input DataSet,
	prog chan string, group *sync.WaitGroup)  {
	var keys []uint64
	var vals [][]byte

	space1 := make([]byte, 1024)
	rbr := client.NewRubiksR()
	lat := NewLatency()
	defer group.Done()

	for pair := range input.GetKV() {
		keys = append(keys, pair.K)
		vals = append(vals, pair.V)

		if len(vals) == batch {
			kks, vvs := mkKKs(keys, space1), mkVVs(vals)
			t0 := time.Now()

			// RPCGet
			outVVs, err := rubiks.RPCGet(rbr, deadline(), kks)
			if err != nil {
				prog <- fmt.Sprintf("read for write: %s", err)
				keys = nil
				vals = nil
				continue
			}

			// overwrite seqnum
			for i := range outVVs {
				vvs[i].Seqnum = outVVs[i].Seqnum
			}

			// RPCCommit
			_, err = rubiks.RPCCommit(rbr, deadline(), kks, vvs)
			if err != nil {
				prog <- fmt.Sprintf("write: %s", err)
			} else {
				lat.Measure(t0)

				if lat.count == 100 {	// send progress on every 100 RPC
					prog <- fmt.Sprintf("write batch %d: %s", batch, lat)
					lat.Reset()
				}
			}

			keys = nil
			vals = nil
		}
	}
}

func mkKKs(keys []uint64, space []byte) []api.RubiksKK {
	var kks []api.RubiksKK

	// construct kks
	for _, key := range keys {
		kks = append(kks, api.RubiksKK{
			Table: api.Table(1),
			Key:   space[:8],
		})
		space = serd.Put64BE(8, space, key)
	}
	return kks
}

func mkVVs(vals [][]byte) []api.RubiksVV {
	var vvs []api.RubiksVV

	for _, val := range vals {
		vvs = append(vvs, api.RubiksVV{
			Present: true,
			Seqnum:  0,
			Val:     val,
		})
	}
	return vvs
}

func parseInput() (network.EndpointList, int, int, int, DataSet) {
	var epl network.EndpointList
	var dset DataSet

	_ = dset.Set("")	// defaults

	flag.CommandLine.Var(&epl, "e", "rubiks server nominal endpoint")
	flag.CommandLine.Var(&dset, "ds", "data set")
	rr := flag.Int("rr", 0,   "read routines")
	wr := flag.Int("wr", 0,   "write routines")
	batch := flag.Int("b", 1, "rpc batch size")

	flag.Parse()
	rand.Seed(time.Now().Unix())

	if len(epl) == 0 {
		flag.Usage()
		os.Exit(255)
	}
	return epl.Delta(api.PortDelta), *rr, *wr, *batch, dset
}

type Latency struct {
	min   time.Duration
	max   time.Duration
	total time.Duration
	count int
}

func NewLatency() *Latency {
	return &Latency{
		min:   time.Second,
		max:   0,
		total: 0,
		count: 0,
	}
}

func (lat *Latency) Measure(t0 time.Time)  {
	d := time.Now().Sub(t0)

	if lat.max < d {
		lat.max = d
	}
	if lat.min > d {
		lat.min = d
	}
	lat.total += d
	lat.count += 1
}

func (lat *Latency) Reset()  {
	lat.min = time.Second
	lat.max = 0
	lat.total = 0
	lat.count = 0
}

func (lat Latency) String() string {
	if lat.count == 0 {
		return "latency(nil)"
	}
	return fmt.Sprintf("min %s max %s avg %s",
		lat.min, lat.max, lat.total/time.Duration(lat.count))
}
