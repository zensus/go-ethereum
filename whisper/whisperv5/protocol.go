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

package whisperv5

import (
	"fmt"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
)

type envelopesMsg struct {
	Envelopes []*Envelope
}

type trustedEnvelopesMsg struct {
	Envelopes []*Envelope
}

func ShhCodeMap() *protocols.CodeMap {
	ct := protocols.NewCodeMap(ProtocolName, uint(ProtocolVersion), MaxMessageLength)
	var v uint64
	ct.Register(&v, &envelopesMsg{}, &trustedEnvelopesMsg{})
	return ct
}

func Shh(wh *Whisper, localAddr []byte, na adapters.NodeAdapter, m adapters.Messenger) p2p.Protocol {
	// handle handshake
	ct := ShhCodeMap()
	run := func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
		glog.V(6).Infof("shh  protocol starting on %v connected to %v", localAddr, p.ID())

		id := p.ID()
		protoPeer := protocols.NewPeer(p, rw, ct, m, func() { na.Disconnect(id[:]) })

		whisperPeer := newPeer(wh, protoPeer)
		// protocol handshake and its validation

		lhs := ProtocolVersion
		hs, err := protoPeer.Handshake(&lhs)
		if err != nil {
			glog.V(6).Infof("handshake failed: %v", err)
			return err
		}

		rhs := hs.(*uint64)
		err = checkShhHandshake(*rhs)
		if err != nil {
			glog.V(6).Infof("handshake between %v and %v  failed: %v", protoPeer.LocalAddr(), protoPeer.RemoteAddr(), err)
			return err
		}

		wh.peerMu.Lock()
		wh.peers[whisperPeer] = struct{}{}
		wh.peerMu.Unlock()

		defer func() {
			wh.peerMu.Lock()
			delete(wh.peers, whisperPeer)
			wh.peerMu.Unlock()
		}()

		protoPeer.Register(&envelopesMsg{}, whisperPeer.handleEnvelopesMsg)
		protoPeer.Register(&trustedEnvelopesMsg{}, whisperPeer.handleTrustedEnvelopesMsg)

		whisperPeer.start()
		defer whisperPeer.stop()

		return protoPeer.Run()
	}

	return p2p.Protocol{
		Name:    ProtocolName,
		Version: uint(ProtocolVersion),
		Length:  ct.Length(),
		Run:     run,
		// NodeInfo: info,
		// PeerInfo: peerInfo,
	}
}

func (p *Peer) handleEnvelopesMsg(msg interface{}) error {
	e := msg.(*envelopesMsg)
	// inject all envelopes into the internal pool
	for _, envelope := range e.Envelopes {
		if err := p.host.add(envelope); err != nil {
			glog.V(logger.Warn).Infof("%v: bad envelope received: [%v], peer will be disconnected", p.peer, err)
			return fmt.Errorf("invalid envelope: %v", err)
		}
		p.mark(envelope)
		// if wh.mailServer != nil {
		//   wh.mailServer.Archive(envelope)
		// }
	}
	return nil
}

func (p *Peer) handleTrustedEnvelopesMsg(msg interface{}) error {
	e := msg.(*envelopesMsg)

	// peer-to-peer message, sent directly to peer bypassing PoW checks, etc.
	// this message is not supposed to be forwarded to other peers, and
	// therefore might not satisfy the PoW, expiry and other requirements.
	// these messages are only accepted from the trusted peer.
	if p.trusted {
		for _, envelope := range e.Envelopes {
			// VT most certainly we dont need to pass the message code here
			p.host.postEvent(envelope, p2pCode)
		}
	}
	return nil
}

// func (w *Whisper) RequestHistoricMessages(peerID []byte, data []byte) error {
// 	p, err := w.getPeer(peerID)
// 	if err != nil {
// 		return err
// 	}
// 	p.trusted = true
// 	return p.Send(data)
// }

func (w *Whisper) SendP2PMessage(peerID []byte, envelope *Envelope) error {
	p, err := w.getPeer(peerID)
	if err != nil {
		return err
	}
	return p.peer.Send(&envelopesMsg{Envelopes: []*Envelope{envelope}})
}
