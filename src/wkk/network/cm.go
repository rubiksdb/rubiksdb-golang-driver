package network

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
	"wkk/common/log"
	"wkk/common/misc"
)

const (
	ConnAllowance = 20 * time.Millisecond
)

type CM interface {
	Submit(req GenericR, ep Endpoint) error
	WaitForCompletion(req GenericR) error

	RPC(req GenericR, ep Endpoint) error
}

func NewCM(tag string, timeout error, magic uint16, bufsz int) CM {
	return &genericCM{
		tag:     tag,
		timeout: timeout,
		magic:   magic,
		clntId:  rand.Uint64(),
		bufsz:   bufsz,
		wmap:    make(map[string]*wire),
		rmap:    make(map[uint64]GenericR),
	}
}

type genericCM struct {
	tag    	string
	timeout error
	magic   uint16
	reqId   uint64
	clntId 	uint64					// random number
	bufsz  	int						// recv buffer size

	mtx  sync.Mutex
	wmap map[string]*wire		// wire by address
	rmap map[uint64]GenericR 	// request by requestId
}

type wire struct {
	addr  string
	magic uint16
	conn  net.Conn
	data  []byte
	que   chan struct{}
}

func (cm *genericCM) Submit(req GenericR, ep Endpoint) error {
	deadline := req.Deadline()

	cm.mtx.Lock()
	cm.reqId += 1
	requestId := cm.reqId
	w := cm.ensure(ep.String())
	cm.mtx.Unlock()

	// wait for token or timeout
	select {
	case <- w.que:
	case <- time.After(deadline.Sub(time.Now())):
		return cm.timeout
	}
	conn := w.conn
	serialized := req.Serialize(requestId, cm.clntId)

	err := w.send(serialized, deadline)
	if err != nil {
		goto out
	}

	cm.mtx.Lock()
	cm.rmap[requestId] = req
	cm.mtx.Unlock()

	// new connection
	if conn != w.conn {
		go cm.poll(w)
	}

out:
	if err != nil {
		w.disconnect(fmt.Sprintf("%v", err))
	}
	w.que <- struct{}{}
	return err
}

func (cm *genericCM) WaitForCompletion(req GenericR) error {
	var err error

	select {
	case <- req.Wakeup():

	case <- time.After(req.Deadline().Sub(time.Now())):
		err = cm.timeout
	}

	cm.mtx.Lock()
	defer cm.mtx.Unlock()

	delete(cm.rmap, req.RequestId())
	return err
}

func (cm *genericCM) RPC(req GenericR, ep Endpoint) error {
	if err := cm.Submit(req, ep); err != nil {
		return err
	}
	return cm.WaitForCompletion(req)
}

func (cm *genericCM) ensure(addr string) *wire {
	if _, ok := cm.wmap[addr]; !ok {
		w := &wire{
			addr:  addr,
			magic: cm.magic,
			conn:  nil,
			data:  make([]byte, cm.bufsz),
			que:   make(chan struct{}, 1),
		}
		w.que <- struct{}{} // token

		cm.wmap[addr] = w
		return w
	}
	return cm.wmap[addr]
}

func (cm *genericCM) poll(w *wire) {
	conn, data, addr := w.conn, w.data, w.addr

	// poll all the way until connection is down
	for avail := 0; w.conn == conn; {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))

		if produced, err := conn.Read(data[avail:]); err != nil {
			if err, ok := err.(net.Error); !ok || !err.Timeout() {
				w.disconnect(fmt.Sprintf("err=%v", err))
				break
			}
		} else {
			avail += produced
		}

		n, requestId, clientId := Consumable(data[:avail], cm.magic)
		if n > 0 {
			if clientId != cm.clntId {
				log.Warn("mall formed response message, teardown connection!!")
				break
			}

			cm.wakeup(requestId, data[:n], addr)

			if avail > n {
				remain := avail - n
				copy(data[:remain], data[n:avail])
			} else {
				misc.Assert(avail == n)
			}
			avail -= n
		}
	}
}

func (cm *genericCM) wakeup(requestId uint64, src []byte, addr string)  {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()

	if req, ok := cm.rmap[requestId]; ok {
		if err := req.Deserialize(src); err != nil {
			if w, ok := cm.wmap[addr]; ok {
				w.disconnect("malformed message received")
			}
		}
		req.Wakeup() <- struct{}{}
	}
}

func (w *wire) disconnect(what string) {
	if w.conn != nil {
		_ = w.conn.Close()
		w.conn = nil

		log.Info("end of connection to %s: %s", w.addr, what)
	}
}

func (w *wire) send(src []byte, deadline time.Time) error {
	// ensure connection
	if w.conn == nil {
		conn, err := net.DialTimeout("tcp", w.addr, ConnAllowance)
		if err != nil {
			log.Warn("err=%v", err)
			return err
		}
		w.conn = conn
	}

	_ = w.conn.SetWriteDeadline(deadline)
	for len(src) > 0 {
		if n, err := w.conn.Write(src); err != nil {
			return err
		} else {
			src = src[n:]
		}
	}
	return nil
}

//func (w *wire) recv(deadline time.Time) ([]byte, error) {
//	_ = w.conn.SetReadDeadline(deadline)
//
//	for avail, data, conn := 0, w.data, w.conn; ; {
//		n, err := conn.Read(data[avail:])
//		if err != nil {
//			return nil, err
//		}
//
//		avail += n
//		if consumable, _, _ := Consumable(data[:avail], w.magic); consumable > 0 {
//			return data[:consumable], nil
//		}
//	}
//}
