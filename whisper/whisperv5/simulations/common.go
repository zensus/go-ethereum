// +build none

// You can run this simulation using
//
//    go run .whisper/whisperv5/simulations/network.go
package main

import (
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/simulations"
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
