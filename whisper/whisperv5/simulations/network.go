// +build none

// You can run this simulation using
//
//    go run .whisper/whisperv5/simulations/network.go
package main

import (
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
	whisper "github.com/ethereum/go-ethereum/whisper/whisperv5"
)

// NewSessionController sits as the top-most controller for this simulation
// creates an inprocess simulation of basic node running their own bzz+hive
func NewSessionController() (*simulations.ResourceController, chan bool) {
	quitc := make(chan bool)
	return simulations.NewResourceContoller(
		&simulations.ResourceHandlers{
			// POST /
			Create: &simulations.ResourceHandler{
				Handle: func(msg interface{}, parent *simulations.ResourceController) (interface{}, error) {
					conf := msg.(*simulations.NetworkConfig)
					messenger := &adapters.SimPipe{}
					net := simulations.NewNetwork(nil, &event.TypeMux{})
					whnet := NewNetwork(net, messenger)
					c := simulations.NewNetworkController(conf, net.Events(), simulations.NewJournal())
					if len(conf.Id) == 0 {
						conf.Id = fmt.Sprintf("%d", 0)
					}
					glog.V(6).Infof("new network controller on %v", conf.Id)
					if parent != nil {
						parent.SetResource(conf.Id, c)
					}
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
							panic(err.Error())
						}
					}

					sender := whnet.nodes[0]
					recip := whnet.nodes[RecipId]
					rid, err := crypto.GenerateKey()
					if err != nil {
						return struct{}{}, fmt.Errorf("generate failed")
					}
					f := whisper.Filter{KeyAsym: rid}
					fid := recip.Watch(&f)
					go func() {
						for {
							time.Sleep(time.Millisecond * 100)
							msgs := recip.Messages(fid)
							if len(msgs) != 0 {
								glog.V(whisper.TestDebug).Infof("decrypted message: [%s] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", string(msgs[0].Payload))
								break
							}
						}

						for i, n := range whnet.nodes {
							mail := n.AllMessages()
							if i != RecipId && len(mail) != 0 {
								panic("wrong message received")
							}
						}
					}()

					opt := whisper.MessageParams{Dst: &rid.PublicKey, WorkTime: 2, PoW: 10.01, Payload: []byte("wtf? why do send this bullshit??")}
					m := whisper.NewSentMessage(&opt)
					envelope, err := m.Wrap(&opt)
					if err != nil {
						panic("failed to seal message.")
					}

					//envelope.PoW()
					//envelope.Hash()
					//glog.V(whisper.TestDebug).Infof("wrapped envelope: %v", envelope)

					err = sender.Send(envelope)
					if err != nil {
						glog.V(whisper.TestDebug).Infof("failed to send envelope [%x]: %s", envelope.Hash(), err)
					}

					time.Sleep(time.Second * 2)
					err = whnet.Connect(ids[RecipId], ids[NumNodes-2])
					if err != nil {
						panic(err.Error())
					}

					return struct{}{}, nil
				},
				Type: reflect.TypeOf(&simulations.NetworkConfig{}),
				// Type: reflect.TypeOf(&simulations.NetworkConfig{}),
			},
			// DELETE /
			Destroy: &simulations.ResourceHandler{
				Handle: func(msg interface{}, parent *simulations.ResourceController) (interface{}, error) {
					glog.V(6).Infof("destroy handler called")
					// this can quit the entire app (shut down the backend server)
					quitc <- true
					return struct{}{}, nil
				},
			},
		},
	), quitc
}

// var server
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	glog.SetV(6)
	glog.SetToStderr(true)

	c, quitc := NewSessionController()

	simulations.StartRestApiServer("8888", c)
	// wait until server shuts down
	<-quitc
}
