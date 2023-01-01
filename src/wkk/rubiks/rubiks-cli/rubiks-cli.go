package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"wkk/network"
	"wkk/rubiks/api"
	"wkk/rubiks/client"
)

func deadline() time.Time {
	return time.Now().Add(time.Second)
}

func main() {
	epl, sFlag, args := parseInput()
	//log.PrimeLog2(true, "")

	rbr := client.NewRubiksR()
	rubiks := client.NewRubiksClient1(epl, client.FavoredRetry)

	if len(args) == 0 {
		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Printf("R >>> ")
			_, input := scanner.Scan(), scanner.Text()

			dispatch(rubiks, rbr, sFlag, strings.Split(input, " "))
		}
	} else {
		dispatch(rubiks, rbr, sFlag, args)
	}
}

func dispatch(rubiks client.Rubiks, rbr *client.RubiksR, sFlag bool, args []string)  {
	switch args[0] {
	case "get":
		if len(args[1:]) > api.MaxNPairs {
			fmt.Printf("up to 8 keys allowed!!\n")
			return
		}
		kks, err := parseKKs(args[1:])
		if err != nil {
			fmt.Printf("error: %s!!\n", err)
			return
		}
		vvs, err := rubiks.RPCGet(rbr, deadline(), kks)
		if err != nil {
			fmt.Printf("error: %s!!\n", err)
			return
		}
		printPairs(kks, vvs, sFlag)

	case "commit":
		if len(args[1:]) > api.MaxNPairs {
			fmt.Printf("up to 8 pairs allowd!!\n")
		}
		kks, vvs, err := parsePairs(args[1:])
		if err != nil {
			fmt.Printf("error: %s!!\n", err)
			return
		}

		outVVs, err := rubiks.RPCCommit(rbr, deadline(), kks, vvs)
		if err != nil {
			fmt.Printf("error: %s!!\n", err)
			return
		}
		printPairs(kks, outVVs, sFlag)

	case "next":
		if len(args[1:]) != 1 {
			fmt.Printf("invald cursor key!!\n")
			return
		}

		kks, err := parseKKs(args[1:])
		if err != nil {
			fmt.Printf("invalid cursor key!!\n")
			return
		}

		kks, _, err = rubiks.RPCIterate(rbr, deadline(), kks[0], 1, api.IterateHintAll)
		if err != nil {
			fmt.Printf("error: %s\n", err)
			return
		}
		fmt.Printf("%s\n", kks[0])

	case "prev":
		if len(args[1:]) != 1 {
			fmt.Printf("invald cursor key!!\n")
			return
		}

		kks, err := parseKKs(args[1:])
		if err != nil {
			fmt.Printf("invalid cursor key!!\n")
			return
		}
		kks, _, err = rubiks.RPCIterate(rbr, deadline(),
			kks[0], 1, api.IterateHintAll | api.IterateHintBack)
		if err != nil {
			fmt.Printf("error: %s\n", err)
			return
		}
		fmt.Printf("%s\n", kks[0])

	case "list":
		if len(args[1:]) != 1 {
			fmt.Printf("invalid table!!\n")
			return
		}

		table, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			fmt.Printf("error: %s\n", err)
			return
		}

		count := 0
		cursor := api.RubiksKK{Table: api.Table(table), Key: []byte{}}
		for {
			kks, vvs, err := rubiks.RPCIterate(rbr, deadline(), cursor, 1, api.IterateHintAll)
			if err != nil {
				if err == api.NONEXT {
					fmt.Printf("total - %d\n", count)
				} else {
					fmt.Printf("error: %s, stop!!\n", err)
				}
				break
			}

			printPairs(kks, vvs, sFlag)

			last := kks[len(kks)-1]
			cursor = api.RubiksKK{Table: last.Table, Key: append([]byte{}, last.Key...)}
			count += 1
		}

	case "exit":
		os.Exit(0)

	case "help", "h":

	default:
		fmt.Printf("use `exit` to exit!\n")
	}
}

func parsePairs(args []string) ([]api.RubiksKK, []api.RubiksVV, error) {
	var kks []api.RubiksKK
	var vvs []api.RubiksVV

	for _, s := range args {
		ss := strings.Split(s, "=")
		if len(ss) != 2 {
			return nil, nil, errors.New("bad pair")
		}

		kk, err := parseKK(ss[0])
		if err != nil {
			return nil, nil, err
		}

		vv, err := parseVV(ss[1])
		if err != nil {
			return nil, nil, err
		}

		kks = append(kks, kk)
		vvs = append(vvs, vv)
	}
	return kks, vvs, nil
}

func parseVV(s string) (api.RubiksVV, error) {
	if ss := strings.Split(s, ","); len(ss) != 2 {
		return api.RubiksVV{}, errors.New("bad pair")
	} else {
		seqnum, err := strconv.ParseUint(ss[0], 10, 64)
		if err != nil {
			return api.RubiksVV{}, err
		}

		val, err := hex.DecodeString(ss[1])
		if err != nil {
			return api.RubiksVV{}, err
		}

		return api.RubiksVV{
			Present: len(val) != 0,
			Seqnum:  api.Seqnum(seqnum),
			Val:     val,
		}, nil
	}
}

func parseKKs(args []string) ([]api.RubiksKK, error) {
	var kks []api.RubiksKK

	for _, s := range args {
		if kk, err := parseKK(s); err != nil {
			return nil, err
		} else {
			kks = append(kks, kk)
		}
	}
	return kks, nil
}

func parseKK(s string) (api.RubiksKK, error) {
	if ss := strings.Split(s, ","); len(ss) != 2 {
		return api.RubiksKK{}, errors.New("bad key")
	} else {
		table, err := strconv.ParseUint(ss[0], 10, 64)
		if err != nil {
			return api.RubiksKK{}, err
		}

		key, err := hex.DecodeString(ss[1])
		if err != nil {
			return api.RubiksKK{}, err
		}
		return api.RubiksKK{
			Table: api.Table(table),
			Key:   key,
		}, nil
	}
}

func printPairs(kks []api.RubiksKK, vvs []api.RubiksVV, sFlag bool) {
	for i := range kks {
		if vvs[i].Present && sFlag {
			fmt.Printf("%v: present=1,seqnum=%d,%s \n", kks[i],
				vvs[i].Seqnum, string(vvs[i].Val))
		} else {
			fmt.Printf("%v: %v \n", kks[i], vvs[i])
		}
	}
}

func parseInput() (network.EndpointList, bool, []string) {
	var epl network.EndpointList

	flag.Usage = func() {
		fmt.Printf("rubiks-cli [-e endpoint]+ command ...                \n")
		fmt.Printf("  -e                           rubiks server nomial endpoint \n")
		fmt.Printf("  -string                      print value as string   \n")
		fmt.Printf("  get    table,key             up to 8 keys allowed    \n")
		fmt.Printf("  commit table,key=seqnum,val  up to 8 paris allowed   \n")
		fmt.Printf("  next   table,key             next pair of key        \n")
		fmt.Printf("  prev   table,key             prev pair of key        \n")
		fmt.Printf("  list   table[,prefix]        list pairs              \n")
		fmt.Printf("                                                       \n")
		fmt.Printf("where table/seqnum are 64-bit integer, key/val are binary in hex \n")
		fmt.Printf("where seqnum starts with 1 and bumps monotonically on each commit\n")
		fmt.Printf("                                                       \n")
		fmt.Printf("example 1: rubiks-cli <endpoint> commit 1,01=1,123456789ABCDEF    \n")
		fmt.Printf("example 2: rubiks-cli <endpoint> get 1,01        \n")
	}

	flag.CommandLine.Var(&epl, "e", "rubiks server endpoint list (nominal)")
	sFlag := flag.CommandLine.Bool("string", false, "show value as string")
	flag.Parse()

	return epl.Delta(api.PortDelta), *sFlag, flag.Args()
}