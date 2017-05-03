// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package network

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/pot"
)

const (
	ProtocolName       = "bzz"
	Version            = 0
	NetworkId          = 322 // BZZ in l33t
	ProtocolMaxMsgSize = 10 * 1024 * 1024
)

// bzz is the bzz protocol view of a protocols.Peer (itself an extension of p2p.Peer)
type bzzPeer struct {
	*protocols.Peer
	localAddr  *peerAddr
	*peerAddr  // remote address
	lastActive time.Time
}

func newBzzPeer(p *protocols.Peer, oaddr, uaddr []byte) {
	return &bzzPeer{
		Peer:      p,
		localAddr: &peerAddr{oaddr, uaddr},
	}
}

// LastActive returns the time the peer was seen to send a message
// implements Peer
func (self *bzzPeer) LastActive() time.Time {
	return self.lastActive
}

// implemented by peerAddr
type PeerAddr interface {
	OverlayAddr() []byte
	UnderlayAddr() []byte
	PO(pot.PotVal, int) (int, bool)
	String() string
}

// the Peer interface that peerPool needs
type Peer interface {
	PeerAddr
	ID() discover.NodeID                                  // the key that uniquely identifies the Node for the peerPool
	Send(interface{}) error                               // can send messages
	Drop(error)                                           // disconnect this peer
	Register(interface{}, func(interface{}) error) uint64 // register message-handler callbacks
	DisconnectHook(func(error))
}

// ProtocolService interface for Protocol Subservices
type ProtocolService interface {
	// the messages the module adds to the protocol
	MsgTypes() []interface{}
	// the protocol module service to run on a peer
	NewPeer(Peer) error
}

// NewBzz constructs the Bzz protocol composed of ProtocolService
// returns p2p.Protocol that is to be offered by the node.Service
func NewBzz(oAddr, uAddr []byte, ps ...ProtocolService) *p2p.Protocol {

	// register Msg Types in protocol code map
	ct := protocols.NewCodeMap(ProtocolName, Version, ProtocolMaxMsgSize)
	ct.Register(0, &bzzHandshake{})
	for i, s := range ps {
		ct.Register((i+1)*10, s.MsgTypes()...)
	}

	// set up run function of protocol
	run := func(p *protocols.Peer) error {
		bee := newBzzPeer(p, oAddr, uAddr)
		// protocol handshake and its validation
		// sets remote peer address
		if err := bee.bzzHandshake(); err != nil {
			return fmt.Errorf("handshake error in peer %v: %v", bee.ID(), err)
		}
		// mount external service models on the peer connection (swap, sync, hive)
		for _, s := range ps {
			if err := s.NewPeer(bee); err != nil {
				return fmt.Errorf("protocol service %v failed to start on peer %v", s.Name(), err)
			}
		}
		return bee.Run()
	}

	return protocols.NewProtocol(ProtocolName, Version, run, ct, nil, nil)
}

// peerAddr implements the PeerAddr interface
type peerAddr struct {
	OAddr []byte
	UAddr []byte
}

func (self *peerAddr) OverlayAddr() []byte {
	return self.OAddr
}

func (self *peerAddr) UnderlayAddr() []byte {
	return self.UAddr
}

func (self *peerAddr) PO(val pot.PotVal, pos int) (int, bool) {
	kp := val.(PeerAddr)
	one := kp.OverlayAddr()
	other := self.OAddr
	for i := pos / 8; i < len(one); i++ {
		if one[i] == other[i] {
			continue
		}
		oxo := one[i] ^ other[i]
		start := 0
		if i == pos/8 {
			start = pos % 8
		}
		for j := start; j < 8; j++ {
			if (uint8(oxo)>>uint8(7-j))&0x01 != 0 {
				return i*8 + j, false
			}
		}
	}
	return len(one) * 8, true
}

func (self *peerAddr) String() string {
	return fmt.Sprintf("%x <%x>", self.OAddr, self.UAddr)
}

// TODO: refactor handshake as ProtocolService and
// make it mockable for simpler protocol exchange tests
/*
Handshake

* Version: 8 byte integer version of the protocol
* NetworkID: 8 byte integer network identifier
* Addr: the address advertised by the node including underlay and overlay connecctions
*/
type bzzHandshake struct {
	Version   uint64
	NetworkId uint64
	Addr      *peerAddr
}

//
func (self *bzzHandshake) String() string {
	return fmt.Sprintf("Handshake: Version: %v, NetworkId: %v, Addr: %v", self.Version, self.NetworkId, self.Addr)
}

// bzzHandshake negotiates the bzz master handshake
// and validates the response, returns error when
// mismatch/incompatibility is evident
func (self *bzzPeer) bzzHandshake() error {

	lhs := &bzzHandshake{
		Version:   uint64(Version),
		NetworkId: uint64(NetworkId),
		Addr:      self.localAddr,
	}

	hs, err := self.Handshake(lhs)
	if err != nil {
		return fmt.Errorf("handshake failed: %v", err)
	}

	rhs := hs.(*bzzHandshake)
	self.peerAddr = rhs.Addr
	if err := checkBzzHandshake(rhs); err != nil {
		return fmt.Errorf("handshake between %v and %v  failed: %v", self.localAddr, self.peerAddr, err)
	}
	return nil
}

// checkBzzHandshake checks for the validity and compatibility of the remote handshake
func checkBzzHandshake(rhs *bzzHandshake) error {

	if NetworkId != rhs.NetworkId {
		return fmt.Errorf("network id mismatch %d (!= %d)", rhs.NetworkId, NetworkId)
	}

	if Version != rhs.Version {
		return fmt.Errorf("version mismatch %d (!= %d)", rhs.Version, Version)
	}

	return nil
}

// RandomAddr is a utility method generating an address from a public key
func RandomAddr() *peerAddr {
	key, err := crypto.GenerateKey()
	if err != nil {
		panic("unable to generate key")
	}
	pubkey := crypto.FromECDSAPub(&key.PublicKey)
	var id discover.NodeID
	copy(id[:], pubkey[1:])
	return &peerAddr{
		OAddr: crypto.Keccak256(pubkey[1:]),
		UAddr: id[:],
	}
}

// NodeId transforms the underlay address to an adapters.NodeId
func NewNodeIdFromPeerAddr(addr PeerAddr) *adapters.NodeId {
	return adapters.NewNodeId(addr.UnderlayAddr())
}

// NewPeerAddrFromNodeId constucts a peerAddr from an adapters.NodeId
// the overlay address is derived as the hash of the nodeId
func NewPeerAddrFromNodeId(n *adapters.NodeId) *peerAddr {
	id := n.NodeID
	return &peerAddr{
		OAddr: crypto.Keccak256(id[:]),
		UAddr: id[:],
	}
}
