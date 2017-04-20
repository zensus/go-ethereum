// Copyright 2017 The go-ethereum Authors
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

// provides a binary merkle tree implementation.
// this is a very inefficient implementation
// also incorrect as per the refrence RBMTHasher
// to be removed
package bmt

import (
	"fmt"
	"io"
)

// reuse pool of CBMTree-s
// for sequential read and write
// can not be called concurrently
// can be further appended after Sum
// Reset gives back the CBMTree to the pool
type CBMTHasher struct {
	pool    *CBMTreePool
	bmt     *CBMTree
	index   int    //
	segment []byte // the last open segment
}

// creates a reusable CBMTHasher that uses the CBMTree pool for CBMT resources
func NewCBMTHasher(p *CBMTreePool) *CBMTHasher {
	return &CBMTHasher{
		pool: p,
	}
}

type CBMTNodeHasher struct {
	left, right  chan []byte
	last         chan bool
	level, index int
	data         bool
}

func NewCBMTNodeHasher(level, index int) *CBMTNodeHasher {
	self := &CBMTNodeHasher{
		left:  make(chan []byte),
		right: make(chan []byte),
		last:  make(chan bool),
		level: level,
		index: index,
	}
	return self
}

// pool of CBMTrees to reuse for concurrent CBMT hashing
type CBMTreePool struct {
	c            chan *CBMTree
	hasher       Hasher
	SegmentSize  int
	SegmentCount int
	Size         int
	count        int
}

// create a CBMTree pool with hasher, segment size, segment count and pool size
// on GetCBMTree it reuses free CBMTrees or creates a new one if size is not reached
// TODO: implement purge : after T time of no use, Trees are popped from the pool
// closed (close their quit channel) and discarded while decrementing count
func NewCBMTreePool(hasher Hasher, segmentCount, size int) *CBMTreePool {
	segmentSize := hasher().Size()
	return &CBMTreePool{
		c:            make(chan *CBMTree, size),
		hasher:       hasher,
		SegmentSize:  segmentSize,
		SegmentCount: segmentCount,
		Size:         size,
	}
}

// GetCBMTree blocks until it returns an available CBMTree
// it reuses free CBMTrees or creates a new one if size is not reached
func (self *CBMTreePool) GetCBMTree() *CBMTree {
	var t *CBMTree
	if self.count == self.Size {
		t = <-self.c
	} else {
		select {
		case t = <-self.c:
		default:
			t = NewCBMTree(self.hasher, self.SegmentSize, self.SegmentCount)
			self.count++
		}
	}
	return t
}

// reusable control structure organised in a binary tree
// CBMTHasher uses a CBMTreePool to pick one for each chunk hash
// the Tree is 'locked' while not in the pool
type CBMTree struct {
	datanodes    []*CBMTNodeHasher
	SegmentCount int
	SegmentSize  int
	result       chan []byte
	quit         chan bool
	hasher       Hasher
}

// initialises the hasher by building up the nodes in the tree
// segmentSize can be any positive integer
// segmentCount needs to be positive integer and does not need to be
// a power of two can even be odd number
func NewCBMTree(hasher Hasher, segmentSize, segmentCount int) *CBMTree {
	var prevlevel []*CBMTNodeHasher
	quit := make(chan bool)
	datanodesCount := (segmentCount + 1) / 2
	// var final chan []byte
	// if datanodesCount > 1 {
	// final = make(chan []byte, 1)
	// } else {
	// final = make(chan []byte)
	// }
	final := make(chan []byte)
	var lastc chan bool
	// iterate over levels and creates 2^level nodes
	count := 1
	level := 0
	result := final
	// fmt.Printf("datanodesCount %v\n", datanodesCount)
	for {
		nodes := make([]*CBMTNodeHasher, count)
		for i, _ := range nodes {
			t := NewCBMTNodeHasher(level, i)
			// fmt.Printf("created node level %v, index: %v/%v\n", level, i, count)
			nodes[i] = t
			if count > 1 {
				lastc, result = getChannel(prevlevel, i)
			}
			go func(lc chan bool, rc chan []byte) {
				t.run(hasher, lc, quit, rc, final)
			}(lastc, result)
		}
		prevlevel = nodes
		if count >= datanodesCount {
			break
		}
		level++
		count *= 2
	}
	for _, node := range prevlevel {
		node.data = true
	}
	// the datanode level is the nodes on the last level where
	return &CBMTree{
		datanodes:    prevlevel,
		SegmentCount: segmentCount,
		SegmentSize:  segmentSize,
		result:       final,
		quit:         quit,
	}
}

// usual Hash interface Sum method appends the byte slice to the underlying
// data before it calculates and returns the hash of the chunk
func (self *CBMTHasher) Sum(b []byte) []byte {
	t := self.getTree()
	self.Write(b)
	// if index is at last segment, hash is already calculating, need to wait
	// on result channel only
	// if chunk is not full, we push the latest (possibly short) segment
	i := self.index
	lastc, dc := getChannel(t.datanodes, i)
	// need to push nil signal first so that if result is a right segment
	// we know that the node is last one in its row before both segments complete
	go func() {
		if lastc != nil {
			lastc <- i%2 == 0
		}
		// if len(self.segment) > 0 {
		// fmt.Printf("pushing segment %v: %x\n", self.index, self.segment)
		dc <- self.segment
		// fmt.Printf("pushed segment %v: %x\n", self.index, self.segment)
		// }
	}()
	var result []byte
	for result == nil {
		result = <-t.result
	}
	return result
}

// CBMTHasher implements io.Writer interface
// Write fills the buffer to hash
// with every full segment read, segment is piped to the left/right segment
// channel of the CBMTNodeHasher of the parent level
func (self *CBMTHasher) Write(b []byte) (int, error) {
	l := len(b)
	s := self.segment
	i := self.index
	t := self.getTree()
	section := 2 * t.SegmentSize
	// seccnt := (t.SegmentCount-1)/2 + 1
	seccnt := (t.SegmentCount + 1) / 2
	// if i == seccnt-1 && t.SegmentCount%2 == 1 {
	// 	section /= 2
	// }
	// calculate missing bit to complete current open segment
	rest := section - len(self.segment)
	if l < rest {
		rest = l
	}
	if len(s) == 0 {
		s = b[:rest]
	} else {
		s = append(s, b[:rest]...)
	}
	// read full segments and the last possibly partial segment
	for len(s) == section && len(b) > rest && i < seccnt-1 {
		// fmt.Printf("segment %v\n", i)
		_, c := getChannel(t.datanodes, i)
		c <- s
		// fmt.Printf("pushed segment %v\n", i)
		b = b[rest:]
		rest = section
		if len(b) < section {
			rest = len(b)
		}
		if i == seccnt-1 {
			break
		}
		if rest == 0 {
			break
		}
		s = b[:rest]
		i++
		// if i == seccnt-1 && t.SegmentCount%2 == 1 {
		// 	section /= 2
		// }
	}
	// fmt.Printf("open segment %v len %v\n (data: %x)\n", i, len(s), s)
	self.segment = s
	self.index = i
	// otherwise, we can assume len(s) == 0, so all buffer is read and chunk is not yet full
	return l, nil
}

// reads from io.Reader and appends to the data to hash using Write
// it reads so that chunk to hash is maximum length or reader reaches if EOF
func (self *CBMTHasher) ReadFrom(r io.Reader) (m int64, err error) {
	t := self.getTree()
	chunksize := t.SegmentSize * t.SegmentCount
	section := 2 * t.SegmentSize
	bufsize := chunksize - section*self.index + len(self.segment)
	buf := make([]byte, bufsize)
	var read int
	for {
		var n int
		n, err = r.Read(buf)
		read += n
		if err == io.EOF || read == len(buf) {
			hash := self.Sum(buf[:n])
			if read == len(buf) {
				err = NewEOC(hash)
			}
			break
		}
		if err != nil {
			break
		}
		n, err = self.Write(buf[:n])
		if err != nil {
			break
		}
	}

	return int64(read), err
}

func (self *CBMTHasher) Size() int {
	return self.getTree().hasher().Size()
}

func (self *CBMTHasher) BlockSize() int {
	return self.getTree().hasher().BlockSize()
}

// eof of chunk error implements error interface
type EOC struct {
	Hash []byte // read the hash of the chunk off the error
}

// EOC implements the error interface
func (self *EOC) Error() string {
	return fmt.Sprintf("hasher limit reached, chunk hash: %x", self.Hash)
}

// creates new end of chunk error with the hash
func NewEOC(hash []byte) *EOC {
	return &EOC{hash}
}

// Reset gives back the CBMTree to the pool whereby it unlocks
// it resets tree, segment and index
func (self *CBMTHasher) Reset() {
	if self.bmt != nil {
		self.pool.c <- self.bmt
		self.bmt = nil
	}
	self.index = 0
	self.segment = nil
}

// the loop that runs on each node of the CBMT waiting for left and right segments
// in arbitrary order
// if an empty/nil segment is sent, the node is a last one on their level
// to be called in a separate go routine which quits if quit channel argument is closed
func (self *CBMTNodeHasher) run(hasher Hasher, lastc, quit chan bool, result, final chan []byte) {
	var left, right []byte
	var wait, last bool
	h := hasher()
O:
	for {
		select {
		case <-quit:
			return

		case v := <-self.last:
			// fmt.Printf("pulled bool signal %v level %v index %v\n", v, self.level, self.index)
			if v {
				wait = !wait
			}
			last = true
			right = nil
			continue O

		case left = <-self.left:
			// if 0-length segment is sent to the left channel
			// the left segment is on the right edge and no need to wait for right side
			// fmt.Printf("pulled left segment (%vB) level %v index %v\n (data: %x)\n", len(left), self.level, self.index, left)

		case right = <-self.right:
			// fmt.Printf("pulled right segment (%vB) level %v index %v\n (data: %x)\n", len(right), self.level, self.index, right)

		}

		// for either left/right segment received, toggle the wait bool, resulting in
		// sending exactly after 2 segments. no need to reset wait value
		wait = !wait
		if !wait {
			// fmt.Printf("ready level %v index %v\n", self.level, self.index)
			var res []byte
			if self.data {
				h.Write(left)
				if len(right) > 0 {
					hash := h.Sum(nil)
					h.Reset()
					h.Write(right)
					rhash := h.Sum(nil)
					h.Reset()
					h.Write(hash)
					h.Write(rhash)
					// fmt.Printf("left data hash: %x, will add right data hash: %x, level %v index %v\n (data: %x)\n", hash, rhash, self.level, self.index, right)
				}
				res = h.Sum(nil)
			} else if len(right) > 0 {
				h.Write(left)
				h.Write(right)
				res = h.Sum(nil)
			} else {
				res = left
			}
			// if the node is last on its level, then parent is last too, so pipe nil to
			// appropriate parent result channel
			// except when on datalevel and we push to result channel
			directResult := self.index == 0 // && len(right) == 0
			if last && !directResult && lastc != nil {
				// fmt.Printf("pushed last node signal from level %v index %v\n", self.level, self.index)
				lastc <- self.index%2 == 0
			}
			// the hash of left|right is piped to the result channel (the appropriate left/right
			// channel of the parent node)
			// fmt.Printf("hash %x of level %v index %v\n", res, self.level, self.index)

			if last && directResult {
				// fmt.Printf("hash %x to final\n", res)
				final <- res
			} else {
				// fmt.Printf("hash %x to result\n", res)
				result <- res
			}

			// base hasher is reset, last set to false
			h.Reset()
			last = false
			right = nil
		}
	}
}

func (self *CBMTHasher) getTree() *CBMTree {
	if self.bmt != nil {
		return self.bmt
	}
	t := self.pool.GetCBMTree()
	self.bmt = t
	return t
}

// convenience functiton to pick parent CBMTNodeHasher
// based on the index and the
// slice of CBMTNodeHasher-s representing the parent level
func getChannel(nodes []*CBMTNodeHasher, i int) (chan bool, chan []byte) {
	node := nodes[i/2]
	lastc := node.last
	if i%2 == 0 {
		return lastc, node.left
	}
	return lastc, node.right
}
