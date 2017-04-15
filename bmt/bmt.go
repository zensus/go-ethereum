package storage

// provides a binary merkle tree implementation.

import (
	"fmt"
	"hash"
	"io"
	"strings"
	"sync"
	"sync/atomic"
)

type Hasher func() hash.Hash

// a reusable hasher for fixed maximum size chunks representing a BMT
// implements the hash.Hash interface
// reuse pool of BMTree-s for amortised memory allocation and resource control
// supports order-agnostic concurrent segment writes
// as well as sequential read and write
// can not be called concurrently on more than one chunk
// can be further appended after Sum
// Reset gives back the BMTree to the pool and guaranteed to leave
// the tree and itself in a state reusable for hashing a new chunk
type BMTHasher struct {
	pool      *BMTreePool // BMT resource pool
	bmt       *BMTree     // prebuilt BMT resource for flowcontrol and proofs
	blocksize int         // segment size (size of hash) also for hash.Hash
	count     int         // segment count
	size      int         // for hash.Hash same as hashsize
	cur       int         // cursor position for righmost currently open chunk
	segment   []byte      // the rightmost open segment (not complete)
	result    chan []byte // result channel
	max       int32       // max segments for SegmentWriter interface
}

// creates a reusable BMTHasher
// implements the hash.Hash interface
// pulls a new BMTree from a resource pool for hashing each chunk
func NewBMTHasher(p *BMTreePool) *BMTHasher {
	return &BMTHasher{
		pool:      p,
		size:      p.SegmentSize,
		blocksize: p.SegmentSize,
		count:     p.SegmentCount,
		result:    make(chan []byte),
	}
}

// a reuseable segment hasher representing a node in a BMT
// it allows for continued writes after a Sum
// and is left in completely reusable state after Reset
type BMTNodeHasher struct {
	level, index        int            // position of node for information/logging only
	initial             bool           // first and last node
	unbalanced          bool           // indicates if a node has only the left segment
	left, right, parent *BMTNodeHasher // BMT connections
	state               int32          // changed via atomic
	hash                []byte         // to record the result so can append/continue writing after Sum
	root                chan []byte    // to indicate the root node for smaller chunks.
	hasher              hash.Hash      // always reset after cycle of use
}

// constructor for segment hasher nodes in the BMT
func NewBMTNodeHasher(level, index int, parent *BMTNodeHasher, hasher Hasher) *BMTNodeHasher {
	self := &BMTNodeHasher{
		parent:  parent,
		hasher:  hasher(),
		level:   level,
		index:   index,
		initial: index == 0,
	}
	return self
}

// provides a pool of BMTrees used as resources by BMTHasher
// a BMTree popped from the pool is guaranteed to have clean state
// for hashing a new chunk
// BMTHasher Reset releases the BMTree to the pool
type BMTreePool struct {
	lock         sync.Mutex
	c            chan *BMTree
	hasher       Hasher
	SegmentSize  int
	SegmentCount int
	Capacity     int
	count        int
}

// create a BMTree pool with hasher, segment size, segment count and capacity
// on GetBMTree it reuses free BMTrees or creates a new one if size is not reached
func NewBMTreePool(hasher Hasher, segmentCount, capacity int) *BMTreePool {
	segmentSize := hasher().Size()
	return &BMTreePool{
		c:            make(chan *BMTree, capacity),
		hasher:       hasher,
		SegmentSize:  segmentSize,
		SegmentCount: segmentCount,
		Capacity:     capacity,
	}
}

// drains the pool uptil it has no more than n resources
func (self *BMTreePool) Drain(n int) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for len(self.c) > n {
		select {
		case <-self.c:
			self.count--
		default:
		}
	}
}

// blocks until it returns an available BMTree
// it reuses free BMTrees or creates a new one if size is not reached
func (self *BMTreePool) Reserve() *BMTree {
	self.lock.Lock()
	defer self.lock.Unlock()
	var t *BMTree
	if self.count == self.Capacity {
		t = <-self.c
	} else {
		select {
		case t = <-self.c:
		default:
			t = NewBMTree(self.hasher, self.SegmentSize, self.SegmentCount)
			self.count++
		}
	}
	return t
}

// Releases a BMTree to the pool. the BMTree is guaranteed to be in reusable state
// does not need locking
func (self *BMTreePool) Release(t *BMTree) {
	select {
	case self.c <- t: // can never fail but...
	default:
	}
}

// reusable control structure representing a BMT
// organised in a binary tree
// BMTHasher uses a BMTreePool to pick one for each chunk hash
// the BMTree is 'locked' while not in the pool
type BMTree struct {
	leaves []*BMTNodeHasher
	result chan []byte
}

// draws the BMT (badly)
func (self *BMTree) Draw() string {
	var left, right []string
	var anc []*BMTNodeHasher
	for i, n := range self.leaves {
		if i%2 == 0 {
			left = append(left, fmt.Sprintf("%v", hashstr(n.hash)))
			anc = append(anc, n.parent)
		} else {
			right = append(right, fmt.Sprintf("%v", hashstr(n.hash)))
		}
	}
	anc = self.leaves
	var hashes [][]string
	l := 0
	for {
		var nodes []*BMTNodeHasher
		for i, n := range anc {
			if i%2 == 0 {
				if n.parent == nil {
					panic("nooo")
				}
				nodes = append(nodes, n.parent)
			}
		}
		hash := []string{}
		l++
		for i, n := range nodes {
			hash = append(hash, fmt.Sprintf("%v", hashstr(n.hash)))
			if i%2 == 0 && n.right != nil {
				hash = append(hash, "+")
			}
		}
		hash = append(hash, "")
		hashes = append(hashes, hash)
		anc = nodes
		if anc[0].parent == nil {
			break
		}
	}
	total := 60
	del := "                             "
	var rows []string
	for i := len(hashes) - 1; i >= 0; i-- {
		var textlen int
		hash := hashes[i]
		for _, s := range hash {
			textlen += len(s)
		}
		if total < textlen {
			total = textlen + len(hash)
		}
		delsize := (total - textlen) / (len(hash) - 1)
		if delsize > len(del) {
			delsize = len(del)
		}
		row := fmt.Sprintf("%v: %v", len(hashes)-i-1, strings.Join(hash, del[:delsize]))
		rows = append(rows, row)

	}
	rows = append(rows, strings.Join(left, "  "))
	rows = append(rows, strings.Join(right, "  "))
	return strings.Join(rows, "\n") + "\n"
}

// initialises the BMTree by building up the nodes of a BMT
// segment size is stipulated to be the size of the hash
// segmentCount needs to be positive integer and does not need to be
// a power of two and can even be an odd number
// segmentSize * segmentCount determines the maximum chunk size
// hashed using the tree
func NewBMTree(hasher Hasher, segmentSize, segmentCount int) *BMTree {
	n := NewBMTNodeHasher(0, 0, nil, hasher)
	n.root = make(chan []byte)
	prevlevel := []*BMTNodeHasher{n}
	// iterate over levels and creates 2^level nodes
	count := 2
	if count > segmentCount {
		count = segmentCount
	}
	level := 1
	for {
		nodes := make([]*BMTNodeHasher, count)
		for i, _ := range nodes {
			var parent *BMTNodeHasher
			parent = prevlevel[i/2]
			t := NewBMTNodeHasher(level, i, parent, hasher)
			// fmt.Printf("created node level %v, index: %v/%v\n", level, i, count)
			nodes[i] = t
			if i%2 == 0 {
				parent.left = t
			} else {
				parent.right = t
			}
		}
		prevlevel = nodes
		if count >= segmentCount {
			break
		}
		level++
		count *= 2
	}
	// the datanode level is the nodes on the last level where
	return &BMTree{
		leaves: prevlevel,
		result: n.root,
	}
}

// methods needed by hash.Hash

// returns the size
func (self *BMTHasher) Size() int {
	return self.size
}

// returns the block size
func (self *BMTHasher) BlockSize() int {
	return self.blocksize
}

// hash.Hash interface Sum method appends the byte slice to the underlying
// data before it calculates and returns the hash of the chunk
func (self *BMTHasher) Sum(b []byte) (r []byte) {
	t := self.getTree()
	self.Write(b)
	i := self.cur
	if i == 0 {
		h := self.pool.hasher()
		h.Write(self.segment)
		return h.Sum(nil)
	}
	n := t.leaves[i]
	// must run strictly before all nodes calculate
	// datanodes are guaranteed to have a parent
	n.parent.finalise(i%2 == 0, t.result)
	self.writeSegment(i, self.segment)
	return <-t.result
}

// BMTHasher implements the io.Writer interface
// Write fills the buffer to hash
// with every full segment complete launches a hasher go routine
// that shoots up the BMT
func (self *BMTHasher) Write(b []byte) (int, error) {
	self.getTree()
	l := len(b)
	if l <= 0 {
		return 0, nil
	}
	s := self.segment
	i := self.cur
	size := self.size
	// calculate missing bit to complete current open segment
	rest := size - len(s)
	if l < rest {
		rest = l
	}
	s = append(s, b[:rest]...)
	l -= rest
	// read full segments and the last possibly partial segment
	for l > 0 && i < self.count-1 {
		// push all finished chunks we read
		self.writeSegment(i, s)
		l -= size
		if l < 0 {
			size += l
		}
		s = b[rest : rest+size]
		rest += size
		i++
	}
	// fmt.Printf("open segment %v len %v\n (data: %v)\n", i, len(s), hashstr(s))
	self.segment = s
	self.cur = i
	// otherwise, we can assume len(s) == 0, so all buffer is read and chunk is not yet full
	return l, nil
}

// reads from io.Reader and appends to the data to hash using Write
// it reads so that chunk to hash is maximum length or reader reaches if EOF
func (self *BMTHasher) ReadFrom(r io.Reader) (m int64, err error) {
	self.getTree()
	bufsize := self.size*self.count - self.size*self.cur - len(self.segment)
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

// Reset gives back the BMTree to the pool whereby it unlocks
// it resets tree, segment and index
func (self *BMTHasher) Reset() {
	if self.bmt != nil {
		for n := self.bmt.leaves[self.cur]; n != nil; n = n.parent {
			n.unbalanced = false
			if n.right != nil {
				n.right.hash = nil
			}
		}
		self.pool.Release(self.bmt)
		self.bmt = nil
	}
	self.cur = 0
	self.segment = nil
}

type SegmentWriter interface {
	Init(int) // initialises the segment writer with max int chunks
	WriteSegment(int, []byte) error
	Hash() []byte
}

func (self *BMTHasher) Hash() []byte {
	return <-self.result
}

func (self *BMTHasher) Init(i int) {
	self.getTree()
	self.max = int32(i)
}

// implements the SegmentWriter interface ie allows for segments
// to be written concurrently
func (self *BMTHasher) WriteSegment(i int, s []byte) (err error) {
	max := atomic.LoadInt32(&self.max)
	if int(max) <= i {
		return NewEOC(nil)
	}
	rightmost := i == int(max-1)
	last := atomic.AddInt32(&self.max, 1) == max
	if rightmost {
		self.segment = s
	} else {
		err = self.writeSegment(i, s)
		if err != nil {
			return err
		}
	}
	if !last {
		return
	}
	self.bmt.leaves[int(self.max-1)].parent.finalise(i%2 == 0, self.result)
	return self.writeSegment(int(self.max-1), self.segment)
}

func (self *BMTHasher) writeSegment(i int, s []byte) error {
	n := self.bmt.leaves[i]
	go func() {
		n.hash = s
		n.parent.pull(s, i%2 == 0)
	}()
	return nil
}

func (self *BMTNodeHasher) push(i int, s []byte) {
	self.hash = s

	if self.root != nil {
		// reset root to false unless toplevel
		// fmt.Printf("%v/%v push root %v\n", self.level, self.index, hashstr(s))
		r := self.root
		if self.parent != nil {
			self.root = nil
		}
		r <- s
		return
	}
	if self.parent.unbalanced {
		// fmt.Printf("%v/%v pushing into unbalanced nodes parent %v\n", self.level, self.index, hashstr(s))
		self.parent.push(self.parent.index, s)
		return
	}
	// fmt.Printf("%v/%v pulled into parent %v\n", self.level, self.index, hashstr(s))
	self.parent.pull(s, i%2 == 0)
}

func (self *BMTNodeHasher) pull(s []byte, left bool) {
	// always read left segment into the hasher BEFORE toggling state
	if left {
		// fmt.Printf("%v/%v pull from left %v\n", self.level, self.index, hashstr(s))
		self.hasher.Write(s)
	} else {
		// fmt.Printf("%v/%v pull from right %v\n", self.level, self.index, hashstr(s))
	}

	if self.right != nil && !self.unbalanced {
		// fmt.Printf("%v/%v toggle state left=%v\n", self.level, self.index, left)
		if self.toggle() {
			// fmt.Printf("%v/%v terminate first thread left=%v\n", self.level, self.index, left)
			return
		}
		// fmt.Printf("%v/%v second thread adding hash %v left=%v\n", self.level, self.index, hashstr(self.right.hash), left)

		self.hasher.Write(self.right.hash)
	}
	self.hash = self.hasher.Sum(nil)
	self.hasher.Reset()

	self.push(self.index, self.hash)
}

// it is lilke following the zigzags on the tree belonging
// to the final datasegment
func (self *BMTNodeHasher) finalise(left bool, result chan []byte) {

	// return if that actual tree top node is
	if self.root != nil {
		return
	}
	// the root node is a node that is on the left edge and not on the dataside
	// this necessitates that finalise is starting on the parent level
	// if the left edge is reached install the result channel and
	// terminate the finalise routine

	if self.initial {
		self.root = result
		return
	}
	// when the final segment's path is going via left segments
	// the incoming data is pushed to the parent upon pulling the left
	// we do not need toogle the state since this condition is
	// detectable
	self.unbalanced = left
	// fmt.Printf("%v/%v finalise %v\n", self.level, self.index, self.unbalanced)

	self.parent.finalise(self.index%2 == 0, result)
}

// obtains a BMT resource by reserving one from the pool
func (self *BMTHasher) getTree() *BMTree {
	if self.bmt != nil {
		return self.bmt
	}
	t := self.pool.Reserve()
	self.bmt = t
	return t
}

// atomic bool toggle implementing a concurrent reusable bi-state object
// atomic addint with %2 implements atomic bool toggle
// it returns true if the toggler just put it in the active/waiting state
func (self *BMTNodeHasher) toggle() bool {
	return atomic.AddInt32(&self.state, 1)%2 == 1
}
func hashstr(b []byte) string {
	end := len(b)
	if end > 4 {
		end = 4
	}
	return fmt.Sprintf("%x", b[:end])
}
