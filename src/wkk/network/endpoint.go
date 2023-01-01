package network

import (
	"encoding/binary"
	"fmt"
	"net"
)

type Endpoint     net.TCPAddr
type EndpointList []Endpoint

func MkEndpoint(addr uint32, port int) Endpoint {
	s := fmt.Sprintf("%d.%d.%d.%d:%d", (addr >> 24) & 0xFF,
		(addr >> 16) & 0xFF, (addr >> 8) & 0xFF, (addr >> 0) & 0xFF, port)
	addr0, _ := net.ResolveTCPAddr("tcp", s)
	return Endpoint(*addr0)
}

func (t *Endpoint) Addr() *net.TCPAddr {
	return (*net.TCPAddr) (t)
}

func (t *Endpoint) Set(s string) error {
	addr, err := net.ResolveTCPAddr("tcp", s)
	if err == nil {
		*t = Endpoint(*addr)
	}
	return err
}

func (t Endpoint) String() string {
	return t.Addr().String()
}

func (t Endpoint) IpU32() uint32 {
	return binary.BigEndian.Uint32(t.IP.To4())
}

func (t Endpoint) U64() uint64 {
	tmp := binary.BigEndian.Uint32(t.IP.To4())
	return uint64(tmp) << 32 | uint64(t.Port)
}

func (t Endpoint) Delta(delta int) Endpoint {
	t.Port += delta
	return t
}

func (t Endpoint) Equal(other Endpoint) bool {
	return t.String() == other.String()
}

func (t EndpointList) Delta(delta int) EndpointList {
	result := EndpointList{}

	for _, ep := range t {
		result = append(result, ep.Delta(delta))
	}
	return result
}

func (t EndpointList) Equal(other EndpointList) bool {
	if len(t) != len(other) {
		return false
	}

	for i := range t {
		if !t[i].Equal(other[i]) {
			return false
		}
	}
	return true
}

/* flag.Value interface */
func (t *EndpointList) Set(s string) error {
	var ep Endpoint

	err := ep.Set(s)
	if err != nil {
		return err
 	}
 	*t = append(*t, ep)
	return nil
}

func (t EndpointList) String() string {
	result := ""
	for _, ep := range t {
		if result == "" {
			result += ep.String()
		} else {
			result += "," + ep.String()
		}
	}
	return result
}