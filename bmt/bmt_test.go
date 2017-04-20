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

package bmt

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto/sha3"
)

const (
	maxproccnt = 8
	maxpoolcap = 32
)

func testDataReader(l int) (r io.Reader) {
	return io.LimitReader(crand.Reader, int64(l))
}

func TestOBMTHasher(t *testing.T) {
	for n := 0; n <= 4096; n += 1 {
		// fmt.Println("chunksize", n)
		testOBMTHasherprv(n, t)
	}
}

func testOBMTHasherprv(n int, t *testing.T) {

	data := make([]byte, n)
	tdata := testDataReader(n)
	tdata.Read(data)

	var tree *BTree
	var r *Root
	var count int
	var err1 error
	start := time.Now()
	tree, r, count, err1 = BuildBMT(sha3.NewKeccak256, data, true)
	elapsed := time.Since(start)
	_ = elapsed
	// log.Printf("n=%d took %s", n, elapsed)

	if err1 != nil {
		fmt.Println(tree, r, count, err1)
		return
	}
	// for i := 0; i < count; i++ {
	// 	p, err := tree.InclusionProof(i)
	// 	if err != nil {
	// 		fmt.Println("proof failed ", i, err.Error())
	// 		continue
	// 	}
	// 	ok, err := r.CheckProof(sha3.NewKeccak256, p.proof, i)
	//
	// 	if !ok || (err != nil) {
	// 		t.Errorf("proof %d failed", i)
	// 	}
	// }

	offset := rand.Intn(n)
	length := rand.Intn((n-offset+1)-1) + 1
	p, err := tree.GetInclusionProofs(offset, length)
	if err != nil {
		t.Errorf("proof %d failed %s", offset, err)
		return

	}

	ok, err := r.CheckProofs(sha3.NewKeccak256, p)

	if !ok || (err != nil) {
		t.Errorf("proof  failed %s", err)
	} else {
		// fmt.Println("proofs ok for offset", offset, "lenght", length, "chunksize", n)
	}

	// ok, err := r.CheckProof(sha3.NewKeccak256, p.proof, i)
	//
	// if !ok || (err != nil) {
	// 	t.Errorf("proof %d failed", i)
	// }

	// fmt.Println("done")
}

func TestBMTHasherCorrectness(t *testing.T) {
	err := testHasher(testBMTHasher)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCBMTHasherCorrectness(t *testing.T) {
	t.Skip("not maintained")
	err := testHasher(testCBMTHasher)
	if err != nil {
		t.Fatal(err)
	}
}

func testHasher(f func(Hasher, []byte, int, int) error) error {
	tdata := testDataReader(4128)
	data := make([]byte, 4128)
	tdata.Read(data)
	hasher := sha3.NewKeccak256
	size := hasher().Size()
	counts := []int{1, 2, 3, 4, 5, 8, 16, 32, 64, 128}

	var err error
	for _, count := range counts {
		max := count * size
		incr := max/size + 1
		for n := 0; n <= max+incr; n += incr {
			err = f(hasher, data, n, count)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func TestBMTHasherReuseWithoutRelease(t *testing.T) {
	testBMTHasherReuse(t, false)
}

func TestBMTHasherReuseWithRelease(t *testing.T) {
	testBMTHasherReuse(t, true)
}

func testBMTHasherReuse(t *testing.T, release bool) {
	hasher := sha3.NewKeccak256
	pool := NewBMTreePool(hasher, 128, 1)
	defer pool.Drain(0)
	bmt := NewBMTHasher(pool, true)

	for i := 0; i < 500; i++ {
		n := rand.Intn(4096)
		tdata := testDataReader(n)
		data := make([]byte, n)
		tdata.Read(data)

		err := testBMTHasherCorrectness(bmt, hasher, data, n, 128)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBMTHasherConcurrency(t *testing.T) {
	hasher := sha3.NewKeccak256
	pool := NewBMTreePool(hasher, 128, maxproccnt)
	defer pool.Drain(0)
	wg := sync.WaitGroup{}
	cycles := 100
	wg.Add(maxproccnt * cycles)
	errc := make(chan error)

	for p := 0; p < maxproccnt; p++ {
		bmt := NewBMTHasher(pool, true)
		go func() {
			for i := 0; i < cycles; i++ {
				n := rand.Intn(4096)
				tdata := testDataReader(n)
				data := make([]byte, n)
				tdata.Read(data)
				err := testBMTHasherCorrectness(bmt, hasher, data, n, 128)
				wg.Done()
				if err != nil {
					errc <- err
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(errc)
	}()
	var err error
	select {
	case <-time.NewTimer(5 * time.Second).C:
		err = fmt.Errorf("timed out")
	case err = <-errc:
	}
	if err != nil {
		t.Fatal(err)
	}
}
func testBMTHasher(hasher Hasher, d []byte, n, count int) error {
	pool := NewBMTreePool(hasher, count, 1)
	defer pool.Drain(0)
	bmt := NewBMTHasher(pool, false)
	return testBMTHasherCorrectness(bmt, hasher, d, n, count)
}

func testCBMTHasher(hasher Hasher, d []byte, n, count int) error {
	pool := NewCBMTreePool(hasher, count, 1)
	bmt := NewCBMTHasher(pool)
	return testBMTHasherCorrectness(bmt, hasher, d, n, ((count+1)/2)*2)
}

func testBMTHasherCorrectness(bmt hash.Hash, hasher Hasher, d []byte, n, count int) (err error) {
	// fmt.Printf("\ndatasize: %v, segment count: %v\n", n, count)
	data := d[:n]
	rbmt := NewRBMTHasher(hasher, count)
	exp := rbmt.Hash(data)
	timeout := time.NewTimer(time.Second)
	c := make(chan error)
	go func() {
		got := bmt.Sum(data)
		// fmt.Printf("result %x\n", got)
		if !bytes.Equal(got, exp) {
			var t string
			node, ok := bmt.(*BMTHasher)
			if ok && node.bmt != nil {
				d := depth(n)
				t = node.bmt.Draw(got, d)
			}
			c <- fmt.Errorf("wrong hash. expected %x, got %x\n%s\n", exp, got, t)
		}

		close(c)
		bmt.Reset()
	}()
	select {
	case _, ok := <-timeout.C:
		if ok {
			err = fmt.Errorf("BMT hash calculation timed out")
		}
	case err = <-c:
	}
	return err
}

func BenchmarkSHA3_4k(t *testing.B)   { benchmarkSHA3(4096, t) }
func BenchmarkSHA3_2k(t *testing.B)   { benchmarkSHA3(4096/2, t) }
func BenchmarkSHA3_1k(t *testing.B)   { benchmarkSHA3(4096/4, t) }
func BenchmarkSHA3_512b(t *testing.B) { benchmarkSHA3(4096/8, t) }
func BenchmarkSHA3_256b(t *testing.B) { benchmarkSHA3(4096/16, t) }
func BenchmarkSHA3_128b(t *testing.B) { benchmarkSHA3(4096/32, t) }

func BenchmarkBMTBaseline_4k(t *testing.B)   { benchmarkBMTBaseline(4096, t) }
func BenchmarkBMTBaseline_2k(t *testing.B)   { benchmarkBMTBaseline(4096/2, t) }
func BenchmarkBMTBaseline_1k(t *testing.B)   { benchmarkBMTBaseline(4096/4, t) }
func BenchmarkBMTBaseline_512b(t *testing.B) { benchmarkBMTBaseline(4096/8, t) }
func BenchmarkBMTBaseline_256b(t *testing.B) { benchmarkBMTBaseline(4096/16, t) }
func BenchmarkBMTBaseline_128b(t *testing.B) { benchmarkBMTBaseline(4096/32, t) }

func BenchmarkRBMTHasher_4k(t *testing.B)   { benchmarkRBMTHasher(4096, t) }
func BenchmarkRBMTHasher_2k(t *testing.B)   { benchmarkRBMTHasher(4096/2, t) }
func BenchmarkRBMTHasher_1k(t *testing.B)   { benchmarkRBMTHasher(4096/4, t) }
func BenchmarkRBMTHasher_512b(t *testing.B) { benchmarkRBMTHasher(4096/8, t) }
func BenchmarkRBMTHasher_256b(t *testing.B) { benchmarkRBMTHasher(4096/16, t) }
func BenchmarkRBMTHasher_128b(t *testing.B) { benchmarkRBMTHasher(4096/32, t) }

func BenchmarkBMTHasher_4k(t *testing.B)   { benchmarkBMTHasher(4096, t) }
func BenchmarkBMTHasher_2k(t *testing.B)   { benchmarkBMTHasher(4096/2, t) }
func BenchmarkBMTHasher_1k(t *testing.B)   { benchmarkBMTHasher(4096/4, t) }
func BenchmarkBMTHasher_512b(t *testing.B) { benchmarkBMTHasher(4096/8, t) }
func BenchmarkBMTHasher_256b(t *testing.B) { benchmarkBMTHasher(4096/16, t) }
func BenchmarkBMTHasher_128b(t *testing.B) { benchmarkBMTHasher(4096/32, t) }

func BenchmarkBMTHasherReuse_4k(t *testing.B)   { benchmarkBMTHasherReuse(4096, t) }
func BenchmarkBMTHasherReuse_2k(t *testing.B)   { benchmarkBMTHasherReuse(4096/2, t) }
func BenchmarkBMTHasherReuse_1k(t *testing.B)   { benchmarkBMTHasherReuse(4096/4, t) }
func BenchmarkBMTHasherReuse_512b(t *testing.B) { benchmarkBMTHasherReuse(4096/8, t) }
func BenchmarkBMTHasherReuse_256b(t *testing.B) { benchmarkBMTHasherReuse(4096/16, t) }
func BenchmarkBMTHasherReuse_128b(t *testing.B) { benchmarkBMTHasherReuse(4096/32, t) }

func BenchmarkOBMTHasher_4k(t *testing.B)   { benchmarkOBMTHasher(4096, t) }
func BenchmarkOBMTHasher_2k(t *testing.B)   { benchmarkOBMTHasher(4096/2, t) }
func BenchmarkOBMTHasher_1k(t *testing.B)   { benchmarkOBMTHasher(4096/4, t) }
func BenchmarkOBMTHasher_512b(t *testing.B) { benchmarkOBMTHasher(4096/8, t) }
func BenchmarkOBMTHasher_256b(t *testing.B) { benchmarkOBMTHasher(4096/16, t) }
func BenchmarkOBMTHasher_128b(t *testing.B) { benchmarkOBMTHasher(4096/32, t) }

//
// func BenchmarkCBMTHasher_4k(t *testing.B)   { benchmarkCBMTHasher(4096, t) }
// func BenchmarkCBMTHasher_2k(t *testing.B)   { benchmarkCBMTHasher(4096/2, t) }
// func BenchmarkCBMTHasher_1k(t *testing.B)   { benchmarkCBMTHasher(4096/4, t) }
// func BenchmarkCBMTHasher_512b(t *testing.B) { benchmarkCBMTHasher(4096/8, t) }
// func BenchmarkCBMTHasher_256b(t *testing.B) { benchmarkCBMTHasher(4096/16, t) }
// func BenchmarkCBMTHasher_128b(t *testing.B) { benchmarkCBMTHasher(4096/32, t) }

// benchmarks the minimum hashing time for a balanced (for simplicity) BMT
// by doing count/segmentsize parallel hashings of 2*segmentsize bytes
// doing it on n maxproccnt each reusing the base hasher
// the premise is that this is the minimum computation needed for a BMT
// therefore this serves as a theoretical optimum for concurrent implementations
func benchmarkBMTBaseline(n int, t *testing.B) {
	tdata := testDataReader(64)
	data := make([]byte, 64)
	tdata.Read(data)
	hasher := sha3.NewKeccak256

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		count := int32((n-1)/hasher().Size() + 1)
		wg := sync.WaitGroup{}
		wg.Add(maxproccnt)
		var i int32
		for j := 0; j < maxproccnt; j++ {
			go func() {
				defer wg.Done()
				h := hasher()
				for atomic.AddInt32(&i, 1) < count {
					h.Write(data)
					h.Sum(nil)
					h.Reset()
				}
			}()
		}
		wg.Wait()
	}
}

func benchmarkOBMTHasher(n int, t *testing.B) {
	tdata := testDataReader(n)
	data := make([]byte, n)
	tdata.Read(data)

	var tree *BTree
	var r *Root
	var count int
	var err1 error

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		tree, r, count, err1 = BuildBMT(sha3.NewKeccak256, data, false)
		if err1 != nil {
			fmt.Println(err1, tree, r, count)
			return
		}
	}
}

func benchmarkCBMTHasher(n int, t *testing.B) {
	tdata := testDataReader(n)
	data := make([]byte, n)
	tdata.Read(data)

	size := 1
	hasher := sha3.NewKeccak256
	segmentCount := 128
	pool := NewCBMTreePool(hasher, segmentCount, size)
	bmt := NewCBMTHasher(pool)

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		bmt.Write(data)
		bmt.Sum(nil)
		bmt.Reset()
	}
}

func benchmarkBMTHasher(n int, t *testing.B) {
	tdata := testDataReader(n)
	data := make([]byte, n)
	tdata.Read(data)

	size := 1
	hasher := sha3.NewKeccak256
	segmentCount := 128
	pool := NewBMTreePool(hasher, segmentCount, size)
	// bmt := NewBMTHasher(pool, false)
	bmt := NewBMTHasher(pool, true)

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		bmt.Write(data)
		bmt.Sum(nil)
		bmt.Reset()
	}
}

func benchmarkBMTHasherReuse(n int, t *testing.B) {
	tdata := testDataReader(n)
	data := make([]byte, n)
	tdata.Read(data)

	size := 2
	hasher := sha3.NewKeccak256
	segmentCount := 128
	pool := NewBMTreePool(hasher, segmentCount, size)
	bmt := NewBMTHasher(pool, false)

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		bmt.Write(data)
		bmt.Sum(nil)
		bmt.Reset()
	}
}

func benchmarkSHA3(n int, t *testing.B) {
	data := make([]byte, n)
	tdata := testDataReader(n)
	tdata.Read(data)
	hasher := sha3.NewKeccak256
	h := hasher()

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		h.Write(data)
		h.Sum(nil)
		h.Reset()
	}
}

func benchmarkRBMTHasher(n int, t *testing.B) {
	data := make([]byte, n)
	tdata := testDataReader(n)
	tdata.Read(data)
	hasher := sha3.NewKeccak256
	rbmt := NewRBMTHasher(hasher, 128)

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		rbmt.Hash(data)
	}
}
