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

	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
	//"github.com/ethereum/go-ethereum/swarm/network"
	whisper "github.com/ethereum/go-ethereum/whisper/whisperv5"
)

// Network extends simulations.Network with hives for each node.
type Network struct {
	*simulations.Network
	nodes     []*whisper.Whisper
	messenger *adapters.SimPipe
}

// SimNode is the adapter used by Swarm simulations.
type SimNode struct {
	node *whisper.Whisper
	adapters.NodeAdapter
}

// the hive update ticker for hive
func af() <-chan time.Time {
	return time.NewTicker(5 * time.Second).C
}

// Start() starts up the hive
// makes SimNode implement *NodeAdapter
func (self *SimNode) Start() error {
	//connect := func(s string) error {
	//	id := network.HexToBytes(s)
	//	return self.Connect(id)
	//}
	// return self.node.Start(nil)
	return nil
}

// Stop() shuts down the hive
// makes SimNode implement *NodeAdapter
func (self *SimNode) Stop() error {
	// return self.node.Stop()
	return nil
}

// NewSimNode creates adapters for nodes in the simulation.
func (self *Network) NewSimNode(conf *simulations.NodeConfig) adapters.NodeAdapter {
	id := conf.Id
	na := adapters.NewSimNode(id, self.Network, self.messenger)
	wh := whisper.NewWhisper(nil, id.Bytes(), na, self.messenger)
	self.nodes = append(self.nodes, wh)
	//codeMap := whisper.ShhCodeMap()
	na.Run = whisper.Shh(wh, id.Bytes(), na, self.messenger).Run
	return &SimNode{
		node:        wh,
		NodeAdapter: na,
	}
}

func NewNetwork(network *simulations.Network, messenger *adapters.SimPipe) *Network {
	n := &Network{
		Network:   network,
		messenger: messenger,
	}
	n.SetNaf(n.NewSimNode)
	return n
}

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
					ids := p2ptest.RandomNodeIds(10)
					for _, id := range ids {
						whnet.NewNode(&simulations.NodeConfig{Id: id})
						whnet.Start(id)
						glog.V(6).Infof("node %v starting up", id)
					}
					// the nodes only know about their 2 neighbours (cyclically)
					for i, id := range ids {
						var peerId *adapters.NodeId
						if i == 0 {
							peerId = ids[len(ids)-1]
						} else {
							peerId = ids[i-1]
						}
						err := whnet.Connect(id, peerId)
						if err != nil {
							panic(err.Error())
						}
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
