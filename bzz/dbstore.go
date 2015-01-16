// disk storage layer for the package blockhash
// inefficient work-in-progress version

package bzz

import (
	//	"crypto/sha256"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethutil"
	"github.com/ethereum/go-ethereum/rlp"
	//	"github.com/syndtr/goleveldb/leveldb"
	//	"path"
)

const dbMaxEntries = 5000 // max number of stored (cached) blocks

const gcArraySize = 500
const gcArrayFreeRatio = 10

type gcItem struct {
	idx    uint64
	value  uint64
	idxKey []byte
}

type dpaDBStorage struct {
	dpaStorage
	db *ethdb.LDBDatabase

	accessCnt uint64
	entryCnt  uint64

	dataIdx           uint64
	gcPos, gcStartPos []byte
	gcArray           []*gcItem
}

type dpaDBIndex struct {
	Idx    uint64
	Access uint64
}

func getIndexGCValue(index *dpaDBIndex) uint64 {

	return index.Access

}

func (s *dpaDBStorage) updateIndexAccess(index *dpaDBIndex) {

	s.accessCnt++
	index.Access = s.accessCnt

}

func getIndexKey(hash HashType) []byte {

	key := make([]byte, HashSize+1)
	key[0] = 0
	copy(key[1:HashSize/2+1], hash[HashSize/2:HashSize])
	copy(key[HashSize/2+1:HashSize+1], hash[0:HashSize/2])

	return key
}

func getDataKey(idx uint64) []byte {

	key := make([]byte, 9)
	key[0] = 1
	binary.BigEndian.PutUint64(key[1:9], idx)

	return key
}

func encodeIndex(index *dpaDBIndex) []byte {

	return ethutil.Encode([]interface{}{index.Idx, index.Access})

}

func encodeData(entry *dpaNode) []byte {

	return ethutil.Encode([]interface{}{entry.data, entry.size})

}

func decodeIndex(data []byte, index *dpaDBIndex) {

	dec := rlp.NewStream(bytes.NewReader(data))
	dec.Decode(index)

}

func decodeData(data []byte, entry *dpaNode) {

	var rlpEntry struct {
		Data []byte
		Size uint64
	}

	dec := rlp.NewStream(bytes.NewReader(data))
	dec.Decode(&rlpEntry)
	entry.data = rlpEntry.Data
	entry.size = int64(rlpEntry.Size)
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

func (s *dpaDBStorage) collectGarbage() {

	it := s.db.NewIterator()
	it.Seek(s.gcPos)
	gcnt := 0
	for gcnt < gcArraySize {
		gci := new(gcItem)
		gci.idxKey = it.Key()
		var index dpaDBIndex
		decodeIndex(it.Value(), &index)
		gci.idx = index.Idx
		gci.value = getIndexGCValue(&index)
		s.gcArray[gcnt] = gci
		gcnt++
		it.Next()
	}

	cutidx := gcListSelect(s.gcArray, 0, gcnt-1, gcnt/gcArrayFreeRatio)
	cutval := s.gcArray[cutidx].value

	//fmt.Print(s.entryCnt, " ")

	for i := 0; i < gcnt; i++ {
		if s.gcArray[i].value < cutval {
			s.db.Delete(s.gcArray[i].idxKey)
			s.db.Delete(getDataKey(s.gcArray[i].idx))
			s.entryCnt--
		}
	}

	//fmt.Println(s.entryCnt)

}

func (s *dpaDBStorage) add(entry *dpaStoreReq) {

	data := encodeData(&entry.dpaNode)
	//data := ethutil.Encode([]interface{}{entry})

	if s.entryCnt >= dbMaxEntries {
		s.collectGarbage()
	}

	s.entryCnt++
	s.dataIdx++
	s.db.Put(getDataKey(s.dataIdx), data)

	var index dpaDBIndex
	index.Idx = s.dataIdx
	s.updateIndexAccess(&index)

	idata := encodeIndex(&index)
	s.db.Put(getIndexKey(entry.hash), idata)

}

func (s *dpaDBStorage) find(hash HashType) (entry dpaNode) {

	key := getIndexKey(hash)
	idata, _ := s.db.Get(key)

	var index dpaDBIndex
	decodeIndex(idata, &index)

	s.updateIndexAccess(&index)
	idata = encodeIndex(&index)
	s.db.Delete(key)
	s.db.Put(key, idata)

	data, _ := s.db.Get(getDataKey(index.Idx))

	decodeData(data, &entry)

	return

}

func (s *dpaDBStorage) process_store(req *dpaStoreReq) {

	s.add(req)

	if s.chain != nil {
		s.chain.store_chn <- req
	}

}

func (s *dpaDBStorage) process_retrieve(req *dpaRetrieveReq) {

	entry := s.find(req.hash)

	if entry.data == nil {
		if s.chain != nil {
			s.chain.retrieve_chn <- req
			return
		}
	}

	res := new(dpaRetrieveRes)
	if entry.data != nil {
		res.dpaNode = entry
	}
	res.req_id = req.req_id
	req.result_chn <- res

}

func (s *dpaDBStorage) Init(ch *dpaStorage) {

	s.dpaStorage.Init()

	var err error
	s.db, err = ethdb.NewLDBDatabase("/tmp/bzz")
	if err != nil {
		fmt.Println("/tmp/bzz error:")
		fmt.Println(err)
	}
	if s.db == nil {
		fmt.Println("LDBDatabase is nil")
	}

	s.gcStartPos = make([]byte, HashSize+1)
	s.gcPos = s.gcStartPos

	s.gcArray = make([]*gcItem, gcArraySize)

}

func (s *dpaDBStorage) Run() {

	for {
		bb := true
		for bb {
			select {
			case store := <-s.store_chn:
				s.process_store(store)
			default:
				bb = false
			}
		}
		select {
		case store := <-s.store_chn:
			s.process_store(store)
		case retrv := <-s.retrieve_chn:
			s.process_retrieve(retrv)
		}
	}

}
