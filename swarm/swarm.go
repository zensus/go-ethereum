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

package swarm

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/contracts/ens"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/swarm/api"
	httpapi "github.com/ethereum/go-ethereum/swarm/api/http"
	"github.com/ethereum/go-ethereum/swarm/fuse"
	"github.com/ethereum/go-ethereum/swarm/network"
	"github.com/ethereum/go-ethereum/swarm/services"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

// the swarm stack
type Swarm struct {
	api              *api.Api
	pss              *network.Pss
	protocolServices []ProtocolService
	services         map[int]services.Service
	*services.MultiService
}

// creates a new swarm service instance
// implements node.Service via MultiService
func NewSwarm(ctx *node.ServiceContext, eth ethclient.Client, config *api.Config) (self *Swarm, err error) {
	if bytes.Equal(common.FromHex(config.PublicKey), storage.ZeroKey) {
		return nil, fmt.Errorf("empty public key")
	}
	if bytes.Equal(common.FromHex(config.BzzKey), storage.ZeroKey) {
		return nil, fmt.Errorf("empty bzz key")
	}

	log.Debug(fmt.Sprintf("Setting up Swarm service components"))

	serviceMap := make(map[int]services.Service)
	// TODO: put everything in params
	kp := network.NewKadParams()
	to := network.NewKademlia(
		common.FromHex(config.BzzKey),
		kp,
	)
	// set up the kademlia hive
	hive = network.NewHive(
		config.HiveParams, // configuration parameters
		to,
	)

	hash := storage.MakeHashFunc(config.ChunkerParams.Hash)
	lstore, err := storage.NewLocalStore(hash, config.StoreParams)
	if err != nil {
		return nil, err
	}

	// setup cloud storage internal access layer
	netstore := storage.NewNetStore(hash, lstore, nil, config.StoreParams)

	// set up DPA, the cloud storage local access layer
	dpaChunkStore := storage.NewDpaChunkStore(lstore, netstore)

	// Swarm Hash Merklised Chunking for Arbitrary-length Document/File storage
	dpa = storage.NewDPA(dpaChunkStore, config.ChunkerParams)

	// set up high level api
	transactOpts := bind.NewKeyedTransactor(privateKey)

	// set up ENS contract interface for domain name resolution
	if eth == (*ethclient.Client)(nil) {
		log.Warn("No ENS, please specify non-empty --ethapi to use domain name resolution")
	} else {
		ensi, err = ens.NewENS(transactOpts, config.EnsRoot, ens)
		if err != nil {
			return nil, err
		}
	}
	log.Debug(fmt.Sprintf("-> Swarm Domain Name Registrar @ address %v", config.EnsRoot.Hex()))

	// Manifests for Smart Hosting
	swarmapi = api.NewApi(dpa, ensi)

	sfs = fuse.NewSwarmFS(api)

	// TODO: should come from config
	pssparams := network.NewPssParams()
	pss = network.NewPss(to, pssparams)

	// start swarm http proxy server
	// if config.Port != "" {
	// 	addr := ":" + self.config.Port
	// }
	// 	&httpapi.ServerConfig{
	// 		Addr:       addr,
	// 		CorsString: self.corsString,
	// 	}
	// go httpapi.StartHttpServer(self.api, )
	httpsrv := httpapi.NewServer(api)

	var protocolServices []ProtocolService
	for _, s := range self.services {
		if ps, ok := s.(ProtocolService); ok {
			protocolServices = append(protocolServices, ps)
		}
	}

	return &Swarm{
		MultiService:     services.NewMultiService(ss...),
		protocolServices: protocolServices,
		services:         serviceMap,
	}, nil
}

// implements node.Service interfaceeef
func (self *Swarm) Protocols() []p2p.Protocol {
	return []p2p.Protocol{network.Bzz(self.oaddr, self.uaddr, self.protocolServices...)}
}

// for testing purposes, shold be removed in production environment!!
// pingtopic, _ := network.MakeTopic("pss", 1)
// self.pss.Register(pingtopic, self.pss.GetPingHandler())

// implements node.Service
// Apis returns the RPC Api descriptors the Swarm implementation offers
// func (self *Swarm) APIs() []rpc.API {

// // Local swarm without netStore
// func NewLocalSwarm(datadir, port string) (self *Swarm, err error) {
//
// 	prvKey, err := crypto.GenerateKey()
// 	if err != nil {
// 		return
// 	}
//
// 	config, err := api.NewConfig(datadir, common.Address{}, prvKey, network.NetworkId)
// 	if err != nil {
// 		return
// 	}
// 	config.Port = port
//
// 	dpa, err := storage.NewLocalDPA(datadir)
// 	if err != nil {
// 		return
// 	}
//
// 	self = &Swarm{
// 		api:    api.NewApi(dpa, nil),
// 		config: config,
// 	}
//
// 	return
// }

// // serialisable info about swarm
// type Info struct {
// 	*api.Config
// 	*chequebook.Params
// }
//
// func (self *Info) Info() *Info {
// 	return self
// }
