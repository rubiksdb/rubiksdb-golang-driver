package api

const (
	WireMagic     = uint16(0x43bf)
	PortDelta     = 1
	SerializeSize = 1024
)

const (
	TagKind = 0x00

	TagClusterID     = 0x01
	TagExtentType    = 0x02
	TagExtentID      = 0x03
	TagParticipantID = 0x04

	TagEcrow           = 0x05
	TagParticipantAddr = 0x06
	TagParticipantPort = 0x07

	TagOutcome = 0x10
)

const (
	KindPing               = uint64(0x01)
	KindCreateFirstReplica = uint64(0x11)
	KindCreateExtraReplica = uint64(0x12)
	KindRemoveReplica      = uint64(0x13)
)

type Outcome uint64

const (
	OK		= Outcome(0)
	Timeout = Outcome(2)
	NoSpace = Outcome(4)
	Inval	= Outcome(5)
	Abort   = Outcome(8)
)

func (oc Outcome) Error() string {
	switch oc {
	case Timeout:	return "Timeout"
	case NoSpace:	return "NoSpace"
	case Inval:		return "Inval"
	case Abort:		return "Abort"
	default:		panic("UNREACHABLE")
	}
}