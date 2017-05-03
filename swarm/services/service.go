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

package services

import (
	"fmt"

	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
)

// Service interface forSubservices
type Service interface {
	Name() string
	// the list of RPC descriptors the service provides
	API() *rpc.API
	// Start is called after all services have been constructed and the networking
	// layer was also initialized to spawn any goroutines required by the service.
	Start(server p2p.Server) error
	// Stop terminates all goroutines belonging to the service, blocking until they
	// are all terminated.
	Stop()
}

// MultiService implements the node interface
type MultiService struct {
	services []Service
}

// creates a MultiService of Subservices and a protocol make function
func NewMultiService(services ...Service) *MultiService {
	return &MultiService{services}
}

// Start starts all services
func (m *MultiService) Start(server p2p.Server) error {
	for _, s := range m.services {
		if err := s.Start(server); err != nil {
			return fmt.Errorf("protocol module for service %v failed to start on peer %v", s.Name(), err)
		}
	}
	return nil
}

// Stop stops all services
func (m *MultiService) Stop() {
	for _, s := range m.services {
		s.Stop()
	}
}

// APIs returns rpc API descriptors at most one from each Service
func (m *MultiService) APIs() (apis []rpc.API) {
	for _, s := range m.services {
		if api := s.API(); api != nil {
			apis = append(apis, *api)
		}
	}
	return apis
}
