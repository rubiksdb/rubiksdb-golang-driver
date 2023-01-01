package rubiks_orm

import (
    "testing"
    "time"
    "wkk/common/misc"
)

type TestUser struct {
    EntityBase

    Id   uint64      `primary:"100"`
    FirstName string `index:"101"`
    LastName  string `index:"101"`

    Birth time.Time  `index:"102"`
}

func Test0(t *testing.T)  {
    user0 := TestUser{
        Id:        1,
        FirstName: "Kyle",
        LastName:  "Xu",
        Birth:     time.Now(),
    }

    Register(&TestUser{})

    kk := primaryIndex(&user0)
    misc.Assert(kk.Table == 100 && len(kk.Key) == 8)

    kk = secondaryKK(&user0, "101", false)
    misc.Assert(kk.Table == 101 && len(kk.Key) == 6)

    kk = secondaryKK(&user0, "102", false)
    misc.Assert(kk.Table == 102 && len(kk.Key) == 8)

    vv, err := commitEntity(&user0)
    misc.AssertNilError(err)

    user1 := TestUser{}
    err = decode(&user1, vv)
    misc.AssertNilError(err)

    misc.Assert(user0.GetSeqnum() == user1.GetSeqnum())
    misc.Assert(user0.Id == user1.Id)
    misc.Assert(user0.LastName == user1.LastName)
    misc.Assert(user0.FirstName == user1.FirstName)
    misc.Assert(user0.Birth.Second() == user1.Birth.Second())
}