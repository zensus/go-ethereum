package main

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
	whisper "github.com/ethereum/go-ethereum/whisper/whisperv5"
)

func TestNetworkSimulation(t *testing.T) {
	messenger := &adapters.SimPipe{}
	net := simulations.NewNetwork(nil, &event.TypeMux{})
	whnet := NewNetwork(net, messenger)

	const NumNodes = 32
	const RecipId = NumNodes - 1
	ids := p2ptest.RandomNodeIds(NumNodes)
	for _, id := range ids {
		whnet.NewNode(&simulations.NodeConfig{Id: id})
		whnet.Start(id)
		glog.V(6).Infof("node %v starting up", id)
	}
	// the nodes only know about their 2 neighbours (cyclically)
	for i, id := range ids {
		if i == RecipId {
			continue
		}
		var peerId *adapters.NodeId
		if i == 0 {
			peerId = ids[len(ids)-2]
		} else {
			peerId = ids[i-1]
		}
		err := whnet.Connect(id, peerId)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	sender := whnet.nodes[0]
	recip := whnet.nodes[RecipId]
	rid, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf(err.Error())
	}
	f := whisper.Filter{KeyAsym: rid}
	fid := recip.Watch(&f)
	go func() {
		ticker := time.NewTicker(time.Millisecond * 100)

		cnt := 0
		for _ = range ticker.C {
			msgs := recip.Messages(fid)
			if len(msgs) != 0 {
				glog.V(whisper.TestDebug).Infof("decrypted message: [%s] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", string(msgs[0].Payload))
				break
			}
			cnt++
			if cnt > 40 {
				t.Fatal("timeout: message not received")
			}
		}

		for i, n := range whnet.nodes {
			mail := n.AllMessages()
			if i != RecipId && len(mail) != 0 {
				t.Fatal("wrong message received")
			}
		}
	}()

	opt := whisper.MessageParams{Dst: &rid.PublicKey, WorkTime: 2, PoW: 10.01, Payload: []byte("wtf? why do send this bullshit??")}
	m := whisper.NewSentMessage(&opt)
	envelope, err := m.Wrap(&opt)
	if err != nil {
		t.Fatal("failed to seal message.")
	}

	err = sender.Send(envelope)
	if err != nil {
		glog.V(whisper.TestDebug).Infof("failed to send envelope [%x]: %s", envelope.Hash(), err)
	}

	time.Sleep(time.Second)
	err = whnet.Connect(ids[RecipId], ids[NumNodes-2])
	if err != nil {
		panic(err.Error())
	}
}
