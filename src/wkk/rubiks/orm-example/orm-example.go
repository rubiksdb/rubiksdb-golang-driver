package main

import (
	"flag"
	"fmt"
	"wkk/common/log"
	"wkk/network"
	"wkk/rubiks/api"
	"wkk/rubiks/client"
	"wkk/rubiks/rubiks-orm"
)

type User struct {
	rubiks_orm.EntityBase
	Id     uint64	`primary:"100"`			// primary key with table 100

	Email  string   `index:"101"`

	Street string   `index:"102"`			// combination index of 3 fields
	Town   string   `index:"102"`
	State  string   `index:"102"`

	Name   string	`index:"103"`			//
}

func main() {
	epl := parseInput()

	rubiks := client.NewRubiksClient(epl)
	orm := rubiks_orm.NewRubiksOrm(rubiks)

	rubiks_orm.Register(&User{})

	user0 := User{
		Id:     10,
		Email:  "foo@gmail.com",
		Street: "111 WindsorRidge Dr",
		Town:   "Westboro",
		State:  "MA",
		Name:   "Foo",
	}
	user1 := User{
		Id:     11,
		Email:  "bar@gmail.com",
		Street: "111 WindsorRidge Dr",
		Town:   "Westboro",
		State:  "MA",
		Name:   "Bar",
	}

	// load user0/user1 (and seqnum) from rubiks
	// overrides fields if there are records in rubiks.
	err := orm.Get(&user0, &user1)
	log.FatalIf(err != nil, "error: %s", err)

	log.Info("load from db: %v", user0)
	log.Info("load from db: %v", user1)

	// commit rubiks
	user0.SetPresent(true)
	user1.SetPresent(true)
	err = orm.Commit(&user0, &user1)
	log.FatalIf(err != nil, "error: %s", err)

	log.Info("%v written", user0)
	log.Info("%v written", user1)

	// list by email
	log.Info("-------------------------")
	log.Info("list by email:")
	rc, ec := orm.ListBy(&user0, "101")
	for ent := range rc {
		log.Info("  %v", ent)
	}

	if err := <- ec; err != nil {
		log.Fatal("error=%s", err)
	}

	// list by address
	log.Info("-------------------------")
	log.Info("list by address:")
	rc, ec = orm.ListBy(&user0, "102")
	for ent := range rc {
		log.Info("  %v", ent)
	}

	if err := <- ec; err != nil {
		log.Fatal("error=%s", err)
	}

	// list by name

	log.Info("all done!!")
}

func parseInput() network.EndpointList {
	var epl network.EndpointList

	flag.Usage = func() {
		fmt.Printf("orm-example [-e endpoint]+                     \n")
		fmt.Printf("  -e    rubiks server nominal endpoint         \n")
	}

	flag.CommandLine.Var(&epl, "e", "rubiks server endpoint list (nominal)")
	flag.Parse()

	return epl.Delta(api.PortDelta)
}
