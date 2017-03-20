package network

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
)

const (
	protocolName    = "foo"
	protocolVersion = 42
)

func init() {
	glog.SetV(6)
	glog.SetToStderr(true)
}

// example protocol implementation peer
// message handlers are methods of this
// goal is that we can use the same for "normal" p2p.protocols operations aswell as pss
type PssTestPeer struct {
	*protocols.Peer
	hasProtocol bool
	successC chan bool
}

// example node simulation peer
// modeled from swarm/network/simulations/discovery/discovery_test.go - commit 08b1e42f
type pssTestNode struct {
	*Hive
	*Pss
	adapters.NodeAdapter

	id      *adapters.NodeId
	network *simulations.Network
	trigger chan *adapters.NodeId
	run     adapters.ProtoCall
	ct      *protocols.CodeMap
}

func (n *pssTestNode) Add(peer Peer) error {
	err := n.Hive.Add(peer)
	time.Sleep(time.Second)
	n.triggerCheck()
	return err
}

func (n *pssTestNode) Start() error {
	return n.Hive.Start(n.connectPeer, n.hiveKeepAlive)
}

func (n *pssTestNode) Stop() error {
	n.Hive.Stop()
	return nil
}

func (n *pssTestNode) connectPeer(s string) error {
	return n.network.Connect(n.id, adapters.NewNodeIdFromHex(s))
}

func (n *pssTestNode) hiveKeepAlive() <-chan time.Time {
	return time.Tick(time.Second * 10)
}

func (n *pssTestNode) triggerCheck() {
	// TODO: rate limit the trigger?
	go func() { n.trigger <- n.id }()
}

func (n *pssTestNode) RunProtocol(id *adapters.NodeId, rw, rrw p2p.MsgReadWriter, peer *adapters.Peer) error {
	return n.NodeAdapter.(adapters.ProtocolRunner).RunProtocol(id, rw, rrw, peer)
}

func (n *pssTestNode) ProtoCall() adapters.ProtoCall {
	return n.run
}

func (n *pssTestNode) OverlayAddr() []byte {
	return n.Pss.Overlay.GetAddr().OverlayAddr()
}

func (n *pssTestNode) UnderlayAddr() []byte {
	return n.id.Bytes()
}

// the content of the msgs we're sending in the tests
type PssTestPayload struct {
	Data string
}

func (m *PssTestPayload) String() string {
	return m.Data
}

func TestPssCache(t *testing.T) {
	to, _ := hex.DecodeString("08090a0b0c0d0e0f1011121314150001020304050607161718191a1b1c1d1e1f")
	oaddr, _ := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	uaddr, _ := hex.DecodeString("101112131415161718191a1b1c1d1e1f000102030405060708090a0b0c0d0e0f")
	waittime := 250 * time.Millisecond
	ps := makePss(oaddr, waittime)
	topic, _ := MakeTopic(protocolName, protocolVersion)
	data := []byte("foo")
	msg := &PssMsg{
		Payload: pssEnvelope{
			TTL:         0,
			SenderOAddr: oaddr,
			SenderUAddr: uaddr,
			Topic:       topic,
			Payload:     data,
		},
	}
	msg.SetRecipient(to)

	msgtwo := &PssMsg{
		Payload: pssEnvelope{
			TTL:         0,
			SenderOAddr: uaddr,
			SenderUAddr: oaddr,
			Topic:       topic,
			Payload:     data,
		},
	}
	msgtwo.SetRecipient(to)

	digest := ps.hashMsg(msg)
	digesttwo := ps.hashMsg(msgtwo)

	if digest != 3595343914 {
		t.Fatalf("digest - got: %d, expected: %d", digest, 3595343914)
	}

	if digest == digesttwo {
		t.Fatalf("different msgs return same crc: %d", digesttwo)
	}

	err := ps.addToFwdCache(digest)
	if err != nil {
		t.Fatalf("write to pss cache failed: %v", err)
	}

	if !ps.checkCache(digest) {
		t.Fatalf("message %v should be in cache but checkCache returned false", msg)
	}

	if ps.checkCache(digesttwo) {
		t.Fatalf("message %v should NOT be in cache but checkCache returned true", msgtwo)
	}

	time.Sleep(waittime)
	if ps.checkCache(digest) {
		t.Fatalf("message %v should have expired from cache but checkCache returned true", msg)
	}
}

func TestPssRegisterHandler(t *testing.T) {
	var topic PssTopic
	var err error
	addr := RandomAddr()
	ps := makePss(addr.UnderlayAddr(), 0)
	
	topic, _ = MakeTopic(protocolName, protocolVersion)
	err = ps.Register(topic, func(msg []byte, p *p2p.Peer, sender []byte) error { return nil })
	if err != nil {
		t.Fatalf("couldnt register protocol 'foo' v 42: %v", err)
	}
	
	topic, _ = MakeTopic(protocolName, protocolVersion)
	err = ps.Register(topic, func(msg []byte, p *p2p.Peer, sender []byte) error { return nil })
	if err == nil {
		t.Fatalf("register protocol 'abc..789' v 65536 should have failed")
	}
}

func TestPssAddSingleHandler(t *testing.T) {

	addr := RandomAddr()

	//ps := newPssBase(t, name, version, addr)
	ps := makePss(addr.OverlayAddr(), 0)
	vct := protocols.NewCodeMap(protocolName, uint(protocolVersion), 65535, &PssTestPayload{})

	// topic will be the mapping in pss used to dispatch to the proper handler
	// the dispatcher is protocol agnostic
	topic, _ := MakeTopic(protocolName, protocolVersion)

	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(protocolName, protocolVersion, vct, nil)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(topic, pssprotocol.GetHandler())

	handlefunc := makePssHandleForward(ps)

	newPssProtocolTester(t, ps, addr, 0, handlefunc)
}

// pss simulation test
// (simnodes running protocols)
func TestPssFullSelf(t *testing.T) {

	var action func(ctx context.Context) error
	var check func(ctx context.Context, id *adapters.NodeId) (bool, error)
	var ctx context.Context
	var result *simulations.StepResult
	var timeout time.Duration
	var cancel context.CancelFunc

	var firstpssnode *adapters.NodeId
	var secondpssnode *adapters.NodeId

	vct := protocols.NewCodeMap(protocolName, protocolVersion, 65535, &PssTestPayload{})
	topic, _ := MakeTopic(protocolName, protocolVersion)

	trigger := make(chan *adapters.NodeId)
	net := simulations.NewNetwork(&simulations.NetworkConfig{
		Id:      "0",
		Backend: true,
	})
	testpeers := make(map[*adapters.NodeId]*PssTestPeer)
	nodes := newPssSimulationTester(t, 3, []int{0,2}, net, trigger, vct, protocolName, protocolVersion, &testpeers)
	ids := []*adapters.NodeId{} // ohh risky! but the action for a specific id should come before the expect anyway
	

	action = func(ctx context.Context) error {
		for id, _ := range nodes {
			ids = append(ids, id)
		}
		for i, id := range ids {
			var peerId *adapters.NodeId
			if i != 0 {
				peerId = ids[i-1]
				if err := net.Connect(id, peerId); err != nil {
					return err
				}
			}
		}
		return nil
	}
	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		node, ok := nodes[id]
		if !ok {
			return false, fmt.Errorf("unknown node: %s (%v)", id, node)
		} else {
			glog.V(logger.Detail).Infof("sim check for node %s ok", id)
		}

		return true, nil
	}

	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)

	result = simulations.NewSimulation(net).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: ids,
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}
	cancel()

	nonode := &adapters.NodeId{}
	firstpssnode = nonode
	secondpssnode = nonode

	// first find a node that we're connected to
	for firstpssnode == nonode {
		glog.V(logger.Debug).Infof("PSS kademlia: Waiting for relaypeer for %x close to %x ...", common.ByteLabel(nodes[ids[0]].OverlayAddr()), common.ByteLabel(nodes[ids[1]].OverlayAddr()))
		nodes[ids[0]].Pss.Overlay.EachLivePeer(nodes[ids[2]].OverlayAddr(), 255, func(p Peer, po int) bool {
			for _, id := range ids {
				if id.NodeID == p.ID() {
					glog.V(logger.Debug).Infof("found PSS relay %v (not %v) in routing table of %v", id.NodeID, ids[0].NodeID, firstpssnode.NodeID)
					firstpssnode = id
				}
			}
			if firstpssnode == nonode {
				return true
			}
			return false
		})
		if firstpssnode == nonode {
			time.Sleep(time.Millisecond * 100)
		}
	}

	// then find the node it's connected to
	for secondpssnode == nonode {
		glog.V(logger.Debug).Infof("PSS kademlia: Waiting for recipientpeer for %x close to %x ...", common.ByteLabel(nodes[ids[0]].OverlayAddr()), common.ByteLabel(nodes[ids[2]].OverlayAddr()))
		nodes[firstpssnode].Pss.Overlay.EachLivePeer(nodes[ids[2]].OverlayAddr(), 256, func(p Peer, po int) bool {
			for _, id := range ids {
				if id.NodeID == p.ID() && id.NodeID != ids[0].NodeID {
					glog.V(logger.Warn).Infof("found PSS recipient %v (not %v) in routing table of %v", id.NodeID, ids[0].NodeID, firstpssnode.NodeID)
					secondpssnode = id
				}
			}
			if secondpssnode == nonode {
				return true
			}
			return false
		})
		if secondpssnode == nonode {
			time.Sleep(time.Millisecond * 100)
		}
	}

	action = func(ctx context.Context) error {
		code, _ := vct.GetCode(&PssTestPayload{})
		msgbytes, _ := makeMsg(code, &PssTestPayload{
			Data: "ping",
		})

		go func() {
			oaddr := nodes[secondpssnode].OverlayAddr()
			err := nodes[ids[0]].Pss.Send(oaddr, topic, msgbytes)
			if err != nil {
				t.Fatalf("could not send pss: %v", err)
			}
			trigger <- ids[0]
		}()

		return nil
	}
	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		
		// also need to know if the protocolpeer is set up
		time.Sleep(time.Millisecond * 100)
		return <- testpeers[ids[0]].successC, nil
		//return true, nil
	}

	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result = simulations.NewSimulation(net).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: []*adapters.NodeId{ids[0]},
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}

	t.Log("Simulation Passed:")
}

func TestPssSimpleSelf(t *testing.T) {
	//var err error
	name := "foo"
	version := 42

	addr := RandomAddr()
	senderaddr := RandomAddr()

	//ps := newPssBase(t, name, version, addr)
	ps := makePss(addr.OverlayAddr(), 0)
	vct := protocols.NewCodeMap(protocolName, protocolVersion, 65535, &PssTestPayload{})

	// topic will be the mapping in pss used to dispatch to the proper handler
	// the dispatcher is protocol agnostic
	topic, _ := MakeTopic(name, version)

	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(name, version, vct, nil)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(topic, pssprotocol.GetHandler())

	handlefunc := makePssHandleForward(ps)

	pt, ct := newPssProtocolTester(t, ps, addr, 2, handlefunc)

	// pss msg we will send
	pssmsg := makeFakeMsg(ps, vct, topic, senderaddr, "ping")

	peersmsgcode, found := ct.GetCode(&peersMsg{})
	if !found {
		t.Fatalf("peersMsg not defined")
	}

	subpeersmsgcode, found := ct.GetCode(&subPeersMsg{})
	if !found {
		t.Fatalf("subPeersMsg not defined")
	}

	pssmsgcode, found := ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	//addr_sim := NewPeerAddrFromNodeId(pt.Ids[1])

	hs_pivot := correctBzzHandshake(addr)

	for _, id := range pt.Ids {
		hs_sim := correctBzzHandshake(NewPeerAddrFromNodeId(id))
		<-pt.GetPeer(id).Connc
		err := pt.TestExchanges(bzzHandshakeExchange(hs_pivot, hs_sim, id)...)
		if err != nil {
			t.Fatalf("Handshake fail: %v", err)
		}

		err = pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: subpeersmsgcode,
						Msg:  &subPeersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("subPeersMsg to peer %v fail: %v", id, err)
		}
	}

	for _, id := range pt.Ids {
		err := pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: peersmsgcode,
						Msg:  &peersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			//	t.Fatalf("peersMsg to peer %v fail: %v", id, err)
		}
	}

	pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg:  pssmsg,
					Peer: pt.Ids[0],
				},
			},
		},
	)

	// wait till pssmsg is processed
	time.Sleep(time.Second)

}

func TestPssSimpleRelay(t *testing.T) {
	//var err error
	name := "foo"
	version := 42

	addr := RandomAddr()
	senderaddr := RandomAddr()

	//ps := newPssBase(t, name, version, addr)
	ps := makePss(addr.OverlayAddr(), 0)
	vct := protocols.NewCodeMap(name, uint(version), 65535, &PssTestPayload{})

	// topic will be the mapping in pss used to dispatch to the proper handler
	// the dispatcher is protocol agnostic
	topic, _ := MakeTopic(name, version)

	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(name, version, vct, nil)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(topic, pssprotocol.GetHandler())

	handlefunc := makePssHandleForward(ps)

	pt, ct := newPssProtocolTester(t, ps, addr, 2, handlefunc)

	// pss msg we will send
	pssmsg := makeFakeMsg(ps, vct, topic, senderaddr, "ping")

	peersmsgcode, found := ct.GetCode(&peersMsg{})
	if !found {
		t.Fatalf("peersMsg not defined")
	}

	subpeersmsgcode, found := ct.GetCode(&subPeersMsg{})
	if !found {
		t.Fatalf("subPeersMsg not defined")
	}

	pssmsgcode, found := ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	//addr_sim := NewPeerAddrFromNodeId(pt.Ids[1])

	hs_pivot := correctBzzHandshake(addr)

	for _, id := range pt.Ids {
		hs_sim := correctBzzHandshake(NewPeerAddrFromNodeId(id))
		<-pt.GetPeer(id).Connc
		err := pt.TestExchanges(bzzHandshakeExchange(hs_pivot, hs_sim, id)...)
		if err != nil {
			t.Fatalf("Handshake fail: %v", err)
		}

		err = pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: subpeersmsgcode,
						Msg:  &subPeersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("subPeersMsg to peer %v fail: %v", id, err)
		}
	}

	for _, id := range pt.Ids {
		err := pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: peersmsgcode,
						Msg:  &peersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			//	t.Fatalf("peersMsg to peer %v fail: %v", id, err)
		}
	}

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg:  pssmsg,
					Peer: pt.Ids[0],
				},
			},

			Expects: []p2ptest.Expect{
				p2ptest.Expect{
					Code:    pssmsgcode,
					Msg:     pssmsg,
					Peer:    pt.Ids[0],
					Timeout: time.Second * 2,
				},
			},
		},
	)

	if err != nil {
		t.Fatalf("PssMsg sending %v to %v (pivot) fail: %v", pt.Ids[0], addr.OverlayAddr(), err)
	}
}

func TestPssProtocolReply(t *testing.T) {
	//var err error
	name := "foo"
	version := 42

	addr := RandomAddr()
	senderaddr := RandomAddr()

	//ps := newPssBase(t, name, version, addr)
	ps := makePss(addr.OverlayAddr(), 0)
	vct := protocols.NewCodeMap(name, uint(version), 65535, &PssTestPayload{})

	// topic is used as a mapping in pss used to dispatch to the proper handler for the pssmsg payload
	// the dispatcher is protocol agnostic

	topic, _ := MakeTopic(name, version)

	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(name, version, vct, nil)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(topic, pssprotocol.GetHandler())

	handlefunc := makePssHandleProtocol(ps)

	pt, ct := newPssProtocolTester(t, ps, addr, 2, handlefunc)

	peersmsgcode, found := ct.GetCode(&peersMsg{})
	if !found {
		t.Fatalf("peersMsg not defined")
	}

	subpeersmsgcode, found := ct.GetCode(&subPeersMsg{})
	if !found {
		t.Fatalf("subPeersMsg not defined")
	}

	pssmsgcode, found := ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	// the pss msg we will send
	pssmsg := makeFakeMsg(ps, vct, topic, senderaddr, "ping")

	//addr_sim := NewPeerAddrFromNodeId(pt.Ids[1])

	hs_pivot := correctBzzHandshake(addr)

	for _, id := range pt.Ids {
		hs_sim := correctBzzHandshake(NewPeerAddrFromNodeId(id))
		<-pt.GetPeer(id).Connc
		err := pt.TestExchanges(bzzHandshakeExchange(hs_pivot, hs_sim, id)...)
		if err != nil {
			t.Fatalf("Handshake fail: %v", err)
		}

		err = pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: subpeersmsgcode,
						Msg:  &subPeersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("subPeersMsg to peer %v fail: %v", id, err)
		}
	}
	/*
		for _, id := range pt.Ids {
			err := pt.TestExchanges(
				p2ptest.Exchange{
					Expects: []p2ptest.Expect{
						p2ptest.Expect{
							Code: peersmsgcode,
							Msg:  &peersMsg{},
							Peer: id,
						},
					},
				},
			)
			if err != nil {
				//	t.Fatalf("peersMsg to peer %v fail: %v", id, err)
			}
		}
	*/

	// we don't want to expect these messages in the sim from now on
	pt.SetIgnoreCodes(subpeersmsgcode, peersmsgcode)

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg:  pssmsg,
					Peer: pt.Ids[0],
				},
			},

			Expects: []p2ptest.Expect{
				p2ptest.Expect{
					Code:    pssmsgcode,
					Msg:     pssmsg,
					Peer:    pt.Ids[0],
					Timeout: time.Second * 2,
				},
			},
		},
	)

	if err != nil {
		t.Fatalf("PssMsg sending %v to %v (pivot) fail: %v", pt.Ids[0], addr.OverlayAddr(), err)
	}
}

// numnodes: how many nodes to create
// pssnodeidx: on which node indices to start the pss
// net: the simulated network
// trigger: hook needed for simulation event reporting
// vct: codemap for virtual protocol
// name: name for virtual protocol (and pss topic)
// version: name for virtual protocol (and pss topic)
// testpeers: pss-specific peers, with hook needed for simulation event reporting

func newPssSimulationTester(t *testing.T, numnodes int, pssnodeidx []int, net *simulations.Network, trigger chan *adapters.NodeId, vct *protocols.CodeMap, name string, version int, testpeers *map[*adapters.NodeId]*PssTestPeer) map[*adapters.NodeId]*pssTestNode {
	topic, _ := MakeTopic(name, version)
	nodes := make(map[*adapters.NodeId]*pssTestNode, numnodes)
	psss := make(map[*adapters.NodeId]*Pss)
	net.SetNaf(func(conf *simulations.NodeConfig) adapters.NodeAdapter {
		var node *pssTestNode
		var handlefunc func(interface{}) error
		addr := NewPeerAddrFromNodeId(conf.Id)
		glog.V(logger.Detail).Infof("Adding sim node oaddr %x uaddr %xx", common.ByteLabel(addr.OverlayAddr()), common.ByteLabel(addr.UnderlayAddr()))
		if (*testpeers)[conf.Id] != nil {
			handlefunc = makePssHandleProtocol(psss[conf.Id])
		} else {
			handlefunc = makePssHandleForward(psss[conf.Id])
		}
		node = newPssTester(t, psss[conf.Id], addr, 0, handlefunc, net, trigger)
		nodes[conf.Id] = node
		return node
	})
	ids := adapters.RandomNodeIds(numnodes)
	for i, id := range ids {
		addr := NewPeerAddrFromNodeId(id)
		psss[id] = makePss(addr.OverlayAddr(), 0)
		for _, idx := range pssnodeidx {
			if idx == i {
				(*testpeers)[id] = &PssTestPeer{
					successC: make(chan bool),
				}
				glog.V(logger.Detail).Infof("added testpeer (idx %d): %v id %v to map %x", i, (*testpeers)[id], id, testpeers)
		
				targetprotocol := makeCustomProtocol(name, version, vct, (*testpeers)[id])
				pssprotocol := NewPssProtocol(psss[id], &topic, vct, targetprotocol)
				psss[id].Register(topic, pssprotocol.GetHandler())
				break
			}
		}

		net.NewNode(&simulations.NodeConfig{Id: id})
		if err := net.Start(id); err != nil {
			t.Fatalf("error starting node %s: %s", id.Label(), err)
		}
	}

	return nodes
}

func newPssTester(t *testing.T, ps *Pss, addr *peerAddr, numsimnodes int, handlefunc func(interface{}) error, net *simulations.Network, trigger chan *adapters.NodeId) *pssTestNode {

	ct := BzzCodeMap()
	ct.Register(&peersMsg{})
	ct.Register(&getPeersMsg{})
	ct.Register(&subPeersMsg{})
	ct.Register(&PssMsg{})

	// set up the outer protocol
	hive := NewHive(NewHiveParams(), ps.Overlay)
	nid := adapters.NewNodeId(addr.UnderlayAddr())

	nodeAdapter := adapters.NewSimNode(nid, net)
	node := &pssTestNode{
		Hive:        hive,
		Pss:         ps,
		NodeAdapter: nodeAdapter,
		id:          nid,
		network:     net,
		trigger:     trigger,
		ct:          ct,
	}

	srv := func(p Peer) error {
		p.Register(&PssMsg{}, handlefunc)
		node.Add(p)
		p.DisconnectHook(func(err error) {
			hive.Remove(p)
		})
		return nil
	}

	node.run = Bzz(addr.OverlayAddr(), addr.UnderlayAddr(), ct, srv, nil, nil).Run
	nodeAdapter.Run = node.run

	return node
}

func newPssProtocolTester(t *testing.T, ps *Pss, addr *peerAddr, numsimnodes int, handlefunc func(interface{}) error) (*p2ptest.ProtocolTester, *protocols.CodeMap) {
	testnode := newPssTester(t, ps, addr, numsimnodes, handlefunc, nil, nil)
	ptt := p2ptest.NewProtocolTester(t, NodeId(addr), numsimnodes, testnode.ProtoCall())
	return ptt, testnode.ct
}

func makePss(addr []byte, cachettl time.Duration) *Pss {
	kp := NewKadParams()
	kp.MinProxBinSize = 3

	pp := NewPssParams()
	pp.Cachettl = cachettl
	
	overlay := NewKademlia(addr, kp)
	ps := NewPss(overlay, pp)
	return ps
}

func makeCustomProtocol(name string, version int, ct *protocols.CodeMap, testpeer *PssTestPeer) *p2p.Protocol {
	run := func(p *protocols.Peer) error {
		glog.V(logger.Detail).Infof("running vprotocol: %v", p)
		if testpeer != nil {
			testpeer.Peer = p
		}
		p.Register(&PssTestPayload{}, testpeer.SimpleHandlePssPayload)
		err := p.Run()
		return err
	}

	return protocols.NewProtocol(name, uint(version), run, ct, nil, nil)
}

func makeFakeMsg(ps *Pss, ct *protocols.CodeMap, topic PssTopic, senderaddr PeerAddr, content string) PssMsg {
	data := PssTestPayload{}
	code, found := ct.GetCode(&data)
	if !found {
		return PssMsg{}
	}

	data.Data = content

	rlpbundle, err := makeMsg(code, data)
	if err != nil {
		return PssMsg{}
	}

	pssenv := pssEnvelope{
		SenderOAddr: senderaddr.OverlayAddr(),
		SenderUAddr: senderaddr.UnderlayAddr(),
		Topic:       topic,
		TTL:         DefaultTTL,
		Payload:     rlpbundle,
	}
	pssmsg := PssMsg{
		Payload: pssenv,
	}
	pssmsg.SetRecipient(ps.Overlay.GetAddr().OverlayAddr())
	
	return pssmsg
}

func makePssHandleForward(ps *Pss) func(msg interface{}) error {
	// for the simple check it passes on the message if it's not for us
	return func(msg interface{}) error {
		pssmsg := msg.(*PssMsg)

		if ps.isSelfRecipient(pssmsg) {
			glog.V(logger.Debug).Infof("pss for us .. yay!")
		} else {
			glog.V(logger.Debug).Infof("passing on pss")
			return ps.Forward(pssmsg)
		}
		return nil
	}
}

func makePssHandleProtocol(ps *Pss) func(msg interface{}) error {
	return func(msg interface{}) error {
		pssmsg := msg.(*PssMsg)

		if ps.isSelfRecipient(pssmsg) {
			glog.V(logger.Detail).Infof("pss for us ... let's process!")
			env := pssmsg.Payload
			umsg := env.Payload // this will be rlp encrypted
			f := ps.GetHandler(env.Topic)
			if f == nil {
				return fmt.Errorf("No registered handler for topic '%s'", env.Topic)
			}
			nid := adapters.NewNodeId(env.SenderUAddr)
			p := p2p.NewPeer(nid.NodeID, adapters.Name(nid.Bytes()), []p2p.Cap{})
			return f(umsg, p, env.SenderOAddr)
		} else {
			glog.V(logger.Detail).Infof("pss was for someone else :'(")
			return ps.Forward(pssmsg)
		}
		return nil
	}
}

// echoes an incoming message
// it comes in through
// Any pointer receiver that has protocols.Peer
func (ptp *PssTestPeer) SimpleHandlePssPayload(msg interface{}) error {
	pmsg := msg.(*PssTestPayload)
	glog.V(logger.Detail).Infof("PssTestPayloadhandler got message %v", pmsg)
	if pmsg.Data == "ping" {
		pmsg.Data = "pong"
		glog.V(logger.Detail).Infof("PssTestPayloadhandler reply %v", pmsg)
		ptp.Send(pmsg)
	} else if pmsg.Data == "pong" {
		ptp.successC <- true
	}

	return nil
}
