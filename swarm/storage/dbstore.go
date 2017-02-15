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

// disk storage layer for the package bzz
// DbStore implements the ChunkStore interface and is used by the DPA as
// persistent storage of chunks
// it implements purging based on access count allowing for external control of
// max capacity

package storage

import (
	"bytes"
	"encoding/binary"
	// "fmt"
	"sync"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	defaultDbCapacity = 5000000
	defaultRadius     = 0 // not yet used

	gcArraySize      = 10000
	gcArrayFreeRatio = 0.1

	// key prefixes for leveldb storage
	kpIndex = 0
	kpData  = 1
)

var (
	keyOldData   = byte(1)
	keyAccessCnt = []byte{2}
	keyEntryCnt  = []byte{3}
	keyDataIdx   = []byte{4}
	keyGCPos     = []byte{5}
	keyData      = byte(6)
)

type gcItem struct {
	idx    uint64
	value  uint64
	idxKey []byte
}

type DbStore struct {
	db *LDBDatabase

	// this should be stored in db, accessed transactionally
	entryCnt, accessCnt, dataIdx, capacity uint64

	gcPos, gcStartPos []byte
	gcArray           []*gcItem

	hashfunc Hasher

	lock sync.Mutex
}

func NewDbStore(path string, hash Hasher, capacity uint64, po func(Key) uint8) (*DbStore, error) {

	db, err := NewLDBDatabase(path)
	if err != nil {
		return nil, err
	}

	s := &DbStore{
		hashfunc: hash,
		db:       db,
	}

	s.setCapacity(capacity)

	s.gcStartPos = make([]byte, 1)
	s.gcStartPos[0] = kpIndex
	s.gcArray = make([]*gcItem, gcArraySize)

	data, _ := s.db.Get(keyEntryCnt)
	s.entryCnt = BytesToU64(data)
	data, _ = s.db.Get(keyAccessCnt)
	s.accessCnt = BytesToU64(data)
	data, _ = s.db.Get(keyDataIdx)
	s.dataIdx = BytesToU64(data)
	s.gcPos, _ = s.db.Get(keyGCPos)
	if s.gcPos == nil {
		s.gcPos = s.gcStartPos
	}
	return s, nil
}

type dpaDBIndex struct {
	Idx    uint64
	Access uint64
}

func BytesToU64(data []byte) uint64 {
	if len(data) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(data)
}

func U64ToBytes(val uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, val)
	return data
}

func getIndexGCValue(index *dpaDBIndex) uint64 {
	return index.Access
}

func (s *DbStore) updateIndexAccess(index *dpaDBIndex) {
	index.Access = s.accessCnt
}

func getIndexKey(hash Key) []byte {
	hashSize := len(hash)
	key := make([]byte, hashSize+1)
	key[0] = 0
	copy(key[1:], hash[:])
	return key
}

func getOldDataKey(idx uint64) []byte {
	key := make([]byte, 9)
	key[0] = keyOldData
	binary.BigEndian.PutUint64(key[1:9], idx)

	return key
}

func getDataKey(idx uint64, po uint8) []byte {
	key := make([]byte, 10)
	key[0] = keyData
	key[1] = byte(po)
	binary.BigEndian.PutUint64(key[2:], idx)

	return key
}

func encodeIndex(index *dpaDBIndex) []byte {
	data, _ := rlp.EncodeToBytes(index)
	return data
}

func encodeData(chunk *Chunk) []byte {
	return append(chunk.Key[:], chunk.SData...)
}

func decodeIndex(data []byte, index *dpaDBIndex) error {
	dec := rlp.NewStream(bytes.NewReader(data), 0)
	return dec.Decode(index)
}

func decodeOldData(data []byte, chunk *Chunk) {
	chunk.SData = data
	chunk.Size = int64(binary.LittleEndian.Uint64(data[32:40]))
}

func decodeData(data []byte, chunk *Chunk) {
	chunk.SData = data
	chunk.Size = int64(binary.LittleEndian.Uint64(data[0:8]))
}

func gcListPartition(list []*gcItem, left int, right int, pivotIndex int) int {
	pivotValue := list[pivotIndex].value
	dd := list[pivotIndex]
	list[pivotIndex] = list[right]
	list[right] = dd
	storeIndex := left
	for i := left; i < right; i++ {
		if list[i].value < pivotValue {
			dd = list[storeIndex]
			list[storeIndex] = list[i]
			list[i] = dd
			storeIndex++
		}
	}
	dd = list[storeIndex]
	list[storeIndex] = list[right]
	list[right] = dd
	return storeIndex
}

func gcListSelect(list []*gcItem, left int, right int, n int) int {
	if left == right {
		return left
	}
	pivotIndex := (left + right) / 2
	pivotIndex = gcListPartition(list, left, right, pivotIndex)
	if n == pivotIndex {
		return n
	} else {
		if n < pivotIndex {
			return gcListSelect(list, left, pivotIndex-1, n)
		} else {
			return gcListSelect(list, pivotIndex+1, right, n)
		}
	}
}

func (s *DbStore) collectGarbage(ratio float32) {
	it := s.db.NewIterator()
	it.Seek(s.gcPos)
	if it.Valid() {
		s.gcPos = it.Key()
	} else {
		s.gcPos = nil
	}
	gcnt := 0

	for (gcnt < gcArraySize) && (uint64(gcnt) < s.entryCnt) {

		if (s.gcPos == nil) || (s.gcPos[0] != kpIndex) {
			it.Seek(s.gcStartPos)
			if it.Valid() {
				s.gcPos = it.Key()
			} else {
				s.gcPos = nil
			}
		}

		if (s.gcPos == nil) || (s.gcPos[0] != kpIndex) {
			break
		}

		gci := new(gcItem)
		gci.idxKey = s.gcPos
		var index dpaDBIndex
		decodeIndex(it.Value(), &index)
		gci.idx = index.Idx
		// the smaller, the more likely to be gc'd
		gci.value = getIndexGCValue(&index)
		s.gcArray[gcnt] = gci
		gcnt++
		it.Next()
		if it.Valid() {
			s.gcPos = it.Key()
		} else {
			s.gcPos = nil
		}
	}
	it.Release()

	cutidx := gcListSelect(s.gcArray, 0, gcnt-1, int(float32(gcnt)*ratio))
	cutval := s.gcArray[cutidx].value

	// fmt.Print(gcnt, " ", s.entryCnt, " ")

	// actual gc
	for i := 0; i < gcnt; i++ {
		if s.gcArray[i].value <= cutval {
			s.delete(s.gcArray[i].idx, s.gcArray[i].idxKey, s.po(Key(s.gcPos[1:])))
		}
	}

	// fmt.Println(s.entryCnt)

	s.db.Put(keyGCPos, s.gcPos)
}

func (s *DbStore) Cleanup() {
	//Iterates over the database and checks that there are no faulty chunks
	it := s.db.NewIterator()
	startPosition := []byte{kpIndex}
	it.Seek(startPosition)
	var key []byte
	var errorsFound, total int
	for it.Valid() {
		key = it.Key()
		if (key == nil) || (key[0] != kpIndex) {
			break
		}
		total++
		var index dpaDBIndex
		err := decodeIndex(it.Value(), &index)
		if err != nil {
			it.Next()
			continue
		}
		data, err := s.db.Get(getDataKey(index.Idx, s.po(Key(key[1:]))))
		if err != nil {
			glog.V(logger.Warn).Infof("Chunk %x found but could not be accessed: %v", key[:], err)
			s.delete(index.Idx, getIndexKey(key[1:]), s.po(Key(key[1:])))
			errorsFound++
		} else {
			hasher := s.hashfunc()
			hasher.Write(data[32:])
			hash := hasher.Sum(nil)
			if !bytes.Equal(hash, key[1:]) {
				glog.V(logger.Warn).Infof("Found invalid chunk. Hash mismatch. hash=%x, key=%x", hash, key[:])
				s.delete(index.Idx, getIndexKey(key[1:]), s.po(Key(key[1:])))
			}
		}
		it.Next()
	}
	it.Release()
	glog.V(logger.Warn).Infof("Found %v errors out of %v entries", errorsFound, total)
}

func (s *DbStore) ReIndex() {
	//Iterates over the database and checks that there are no faulty chunks
	it := s.db.NewIterator()
	startPosition := []byte{keyOldData}
	it.Seek(startPosition)
	var key []byte
	var errorsFound, total int
	for it.Valid() {
		key = it.Key()
		if (key == nil) || (key[0] != keyOldData) {
			break
		}
		data := it.Value()
		hasher := s.hashfunc()
		hasher.Write(data)
		hash := hasher.Sum(nil)
		newData := append(data, hash...)

		newKey := make([]byte, 10)
		key[0] = keyData
		key[1] = byte(s.po(Key(key[1:])))
		copy(newKey[2:], key[1:])
		newValue := append(hash, data...)

		batch := new(leveldb.Batch)
		batch.Delete(key)
		batch.Put(newKey, newValue)
		s.db.Write(batch)
		it.Next()
	}
	it.Release()
	glog.V(logger.Warn).Infof("Found %v errors out of %v entries", errorsFound, total)
}

func (s *DbStore) delete(idx uint64, idxKey []byte, po uint8) {
	batch := new(leveldb.Batch)
	batch.Delete(idxKey)
	batch.Delete(getDataKey(idx, po))
	s.entryCnt--
	batch.Put(keyEntryCnt, U64ToBytes(s.entryCnt))
	s.db.Write(batch)
}

func (s *DbStore) Counter() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.dataIdx
}

func (s *DbStore) Put(chunk *Chunk) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ikey := getIndexKey(chunk.Key)
	var index dpaDBIndex

	if s.tryAccessIdx(ikey, &index) {
		if chunk.dbStored != nil {
			close(chunk.dbStored)
		}
		glog.V(logger.Detail).Infof("Storing to DB: chunk already exists, only update access")
		return // already exists, only update access
	}

	data := encodeData(chunk)
	//data := ethutil.Encode([]interface{}{entry})

	if s.entryCnt >= s.capacity {
		s.collectGarbage(gcArrayFreeRatio)
	}

	batch := new(leveldb.Batch)

	batch.Put(getDataKey(s.dataIdx, s.po(chunk.Key)), data)

	index.Idx = s.dataIdx
	s.updateIndexAccess(&index)

	idata := encodeIndex(&index)
	batch.Put(ikey, idata)

	batch.Put(keyEntryCnt, U64ToBytes(s.entryCnt))
	s.entryCnt++
	batch.Put(keyDataIdx, U64ToBytes(s.dataIdx))
	s.dataIdx++
	batch.Put(keyAccessCnt, U64ToBytes(s.accessCnt))
	s.accessCnt++

	s.db.Write(batch)
	if chunk.dbStored != nil {
		close(chunk.dbStored)
	}
	glog.V(logger.Detail).Infof("DbStore.Put: %v. db storage counter: %v ", chunk.Key.Log(), s.dataIdx)
}

// try to find index; if found, update access cnt and return true
func (s *DbStore) tryAccessIdx(ikey []byte, index *dpaDBIndex) bool {
	idata, err := s.db.Get(ikey)
	if err != nil {
		return false
	}
	decodeIndex(idata, index)

	batch := new(leveldb.Batch)

	batch.Put(keyAccessCnt, U64ToBytes(s.accessCnt))
	s.accessCnt++
	s.updateIndexAccess(index)
	idata = encodeIndex(index)
	batch.Put(ikey, idata)

	s.db.Write(batch)

	return true
}

func (s *DbStore) Get(key Key) (chunk *Chunk, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var index dpaDBIndex

	if s.tryAccessIdx(getIndexKey(key), &index) {
		var data []byte
		data, err = s.db.Get(getDataKey(index.Idx, s.po(key)))
		if err != nil {
			glog.V(logger.Detail).Infof("DBStore: Chunk %v found but could not be accessed: %v", key.Log(), err)
			s.delete(index.Idx, getIndexKey(key), s.po(key))
			return
		}

		hasher := s.hashfunc()
		hasher.Write(data)
		hash := hasher.Sum(nil)
		if !bytes.Equal(hash, key) {
			s.delete(index.Idx, getIndexKey(key), s.po(key))
			panic("Invalid Chunk in Database. Please repair with command: 'swarm cleandb'")
		}

		chunk = &Chunk{
			Key: key,
		}
		decodeData(data, chunk)
	} else {
		err = notFound
	}

	return

}

// TODO:
func (s *DbStore) po(key Key) uint8 {
	return 0
}

func (s *DbStore) updateAccessCnt(key Key) {

	s.lock.Lock()
	defer s.lock.Unlock()

	var index dpaDBIndex
	s.tryAccessIdx(getIndexKey(key), &index) // result_chn == nil, only update access cnt

}

func (s *DbStore) setCapacity(c uint64) {

	s.lock.Lock()
	defer s.lock.Unlock()

	s.capacity = c

	if s.entryCnt > c {
		var ratio float32
		ratio = float32(1.01) - float32(c)/float32(s.entryCnt)
		if ratio < gcArrayFreeRatio {
			ratio = gcArrayFreeRatio
		}
		if ratio > 1 {
			ratio = 1
		}
		for s.entryCnt > c {
			s.collectGarbage(ratio)
		}
	}
}

func (s *DbStore) getEntryCnt() uint64 {
	return s.entryCnt
}

func (s *DbStore) Close() {
	s.db.Close()
}

//  describes a section of the DbStore representing the unsynced
// domain relevant to a peer
// Start - Stop designate a continuous area Keys in an address space
// typically the addresses closer to us than to the peer but not closer
// another closer peer in between
// From - To designates a time interval typically from the last disconnect
// till the latest connection (real time traffic is relayed)
type DbSyncState struct {
	Start, Stop Key
	First, Last uint64
}

// initialises a sync iterator from a syncToken (passed in with the handshake)
func (s *DbStore) NewSyncIterator(since uint64, po uint8, f func(key Key) bool) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	it := s.db.NewIterator()
	it.Seek(getDataKey(since, po))
	for it.Valid() {
		dbkey := it.Key()
		if dbkey[0] != keyData || dbkey[1] != byte(po) {
			break
		}
		it.Next()
		if !f(Key(it.Value()[:32])) {
			break
		}
	}
	it.Release()
	return nil
}
