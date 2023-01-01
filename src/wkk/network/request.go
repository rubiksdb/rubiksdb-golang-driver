package network

import (
	"time"
)

type GenericR interface {
	Deadline()  time.Time
	Wakeup()    chan struct{}
	RequestId() uint64

	Serialize(uint64, uint64) []byte

	Deserialize(src []byte) error
}