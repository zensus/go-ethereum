package storage

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"hash"
	"io"
	// "log"
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

func TestBuildBMT(t *testing.T) {
	for n := 0; n <= 4096; n += 1 {
		// fmt.Println("chunksize", n)
		testBuildBMTprv(n, t)
	}
}

func testBuildBMTprv(n int, t *testing.T) {

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
		incr := max/32 + 1
		for n := 0; n <= max+incr; n += incr {
			err = f(hasher, data, n, count)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func TestBMTHasherReuse(t *testing.T) {
	hasher := sha3.NewKeccak256
	pool := NewBMTreePool(hasher, 8, 1)
	defer pool.Drain(0)
	bmt := NewBMTHasher(pool)

	for i := 0; i < 500; i++ {
		n := rand.Intn(256)
		// n := rand.Intn(4096)
		tdata := testDataReader(n)
		data := make([]byte, n)
		tdata.Read(data)
		// t.Logf("datasize: %v", n)
		err := testBMTHasherCorrectness(bmt, hasher, data, n, 128)
		if err != nil {
			pool := NewBMTreePool(hasher, 8, 1)
			defer pool.Drain(0)
			bmt := NewBMTHasher(pool)
			m := bmt.getTree()
			testBMTHasherCorrectness(bmt, hasher, data, n, 128)
			fmt.Printf(m.Draw())
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
		bmt := NewBMTHasher(pool)
		go func() {
			for i := 0; i < cycles; i++ {
				n := rand.Intn(4096)
				tdata := testDataReader(n)
				data := make([]byte, n)
				tdata.Read(data)
				// t.Logf("datasize: %v", n)
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
	case err = <-errc:
	case <-time.NewTimer(5 * time.Second).C:
		err = fmt.Errorf("timed out")
	}
	if err != nil {
		t.Fatal(err)
	}
}
func testBMTHasher(hasher Hasher, d []byte, n, count int) error {
	pool := NewBMTreePool(hasher, count, 1)
	defer pool.Drain(0)
	bmt := NewBMTHasher(pool)
	return testBMTHasherCorrectness(bmt, hasher, d, n, count)
}

func testCBMTHasher(hasher Hasher, d []byte, n, count int) error {
	pool := NewCBMTreePool(hasher, count, 1)
	bmt := NewCBMTHasher(pool)
	return testBMTHasherCorrectness(bmt, hasher, d, n, ((count+1)/2)*2)
}

func testBMTHasherCorrectness(bmt hash.Hash, hasher Hasher, d []byte, n, count int) (err error) {
	// fmt.Printf("datasize: %v, poolsize: 1, segment count: %v\n", n, count)
	data := d[:n]
	span := n
	maxSpan := count * hasher().Size()
	if span > maxSpan {
		span = maxSpan
	}
	exp := SimpleBMT(data[:span], hasher)
	timeout := time.NewTimer(time.Second)
	c := make(chan error)
	go func() {
		got := bmt.Sum(data)
		if !bytes.Equal(got, exp) {
			var t string
			n, ok := bmt.(*BMTHasher)
			if ok && n.bmt != nil {
				t = n.bmt.Draw()
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

func BenchmarkBuildSHA3_4k(t *testing.B)   { benchmarkSHA3(4096, t) }
func BenchmarkBuildSHA3_2k(t *testing.B)   { benchmarkSHA3(4096/2, t) }
func BenchmarkBuildSHA3_1k(t *testing.B)   { benchmarkSHA3(4096/4, t) }
func BenchmarkBuildSHA3_512b(t *testing.B) { benchmarkSHA3(4096/8, t) }
func BenchmarkBuildSHA3_256b(t *testing.B) { benchmarkSHA3(4096/16, t) }
func BenchmarkBuildSHA3_128b(t *testing.B) { benchmarkSHA3(4096/32, t) }

func BenchmarkBMTBaseline_4k(t *testing.B)   { benchmarkBMTBaseline(4096, t) }
func BenchmarkBMTBaseline_2k(t *testing.B)   { benchmarkBMTBaseline(4096/2, t) }
func BenchmarkBMTBaseline_1k(t *testing.B)   { benchmarkBMTBaseline(4096/4, t) }
func BenchmarkBMTBaseline_512b(t *testing.B) { benchmarkBMTBaseline(4096/8, t) }
func BenchmarkBMTBaseline_256b(t *testing.B) { benchmarkBMTBaseline(4096/16, t) }
func BenchmarkBMTBaseline_128b(t *testing.B) { benchmarkBMTBaseline(4096/32, t) }

func BenchmarkSimpleBMT_4k(t *testing.B)   { benchmarkSimpleBMT(4096, t) }
func BenchmarkSimpleBMT_2k(t *testing.B)   { benchmarkSimpleBMT(4096/2, t) }
func BenchmarkSimpleBMT_1k(t *testing.B)   { benchmarkSimpleBMT(4096/4, t) }
func BenchmarkSimpleBMT_512b(t *testing.B) { benchmarkSimpleBMT(4096/8, t) }
func BenchmarkSimpleBMT_256b(t *testing.B) { benchmarkSimpleBMT(4096/16, t) }
func BenchmarkSimpleBMT_128b(t *testing.B) { benchmarkSimpleBMT(4096/32, t) }

func BenchmarkBMTHasher_4k(t *testing.B)   { benchmarkBMTHasher(4096, t) }
func BenchmarkBMTHasher_2k(t *testing.B)   { benchmarkBMTHasher(4096/2, t) }
func BenchmarkBMTHasher_1k(t *testing.B)   { benchmarkBMTHasher(4096/4, t) }
func BenchmarkBMTHasher_512b(t *testing.B) { benchmarkBMTHasher(4096/8, t) }
func BenchmarkBMTHasher_256b(t *testing.B) { benchmarkBMTHasher(4096/16, t) }
func BenchmarkBMTHasher_128b(t *testing.B) { benchmarkBMTHasher(4096/32, t) }

func BenchmarkBuildBMT_4k(t *testing.B)   { benchmarkBuildBMT(4096, t) }
func BenchmarkBuildBMT_2k(t *testing.B)   { benchmarkBuildBMT(4096/2, t) }
func BenchmarkBuildBMT_1k(t *testing.B)   { benchmarkBuildBMT(4096/4, t) }
func BenchmarkBuildBMT_512b(t *testing.B) { benchmarkBuildBMT(4096/8, t) }
func BenchmarkBuildBMT_256b(t *testing.B) { benchmarkBuildBMT(4096/16, t) }
func BenchmarkBuildBMT_128b(t *testing.B) { benchmarkBuildBMT(4096/32, t) }

func BenchmarkCBMTHasher_4k(t *testing.B)   { benchmarkCBMTHasher(4096, t) }
func BenchmarkCBMTHasher_2k(t *testing.B)   { benchmarkCBMTHasher(4096/2, t) }
func BenchmarkCBMTHasher_1k(t *testing.B)   { benchmarkCBMTHasher(4096/4, t) }
func BenchmarkCBMTHasher_512b(t *testing.B) { benchmarkCBMTHasher(4096/8, t) }
func BenchmarkCBMTHasher_256b(t *testing.B) { benchmarkCBMTHasher(4096/16, t) }
func BenchmarkCBMTHasher_128b(t *testing.B) { benchmarkCBMTHasher(4096/32, t) }

// benchmarks the minimum hashing time for a balanced (for simplicity) BMT
// by doing count/segmentsize parallel hashings of 2*segmentsize bytes
// doing it on n maxproccnt each reusing the base hasher
// the premise is that this is the minimum computation needed for a BMT
// therefore this serves as a theoretical optimum for concurrent implementations
func benchmarkBMTBaseline(n int, t *testing.B) {
	tdata := testDataReader(64)
	data := make([]byte, 64)
	tdata.Read(data)
	t.ReportAllocs()
	t.ResetTimer()
	hasher := sha3.NewKeccak256
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

func benchmarkBuildBMT(n int, t *testing.B) {
	//t.ReportAllocs()
	tdata := testDataReader(n)
	data := make([]byte, n)
	tdata.Read(data)

	//reader := bytes.NewReader(data)

	var tree *BTree
	var r *Root
	var count int
	var err1 error
	//	blocks := splitData(data, 32)
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

	t.ReportAllocs()
	t.ResetTimer()
	size := 1
	hasher := sha3.NewKeccak256
	segmentCount := 128
	pool := NewCBMTreePool(hasher, segmentCount, size)
	bmt := NewCBMTHasher(pool)

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

	t.ReportAllocs()
	t.ResetTimer()
	size := 2
	hasher := sha3.NewKeccak256
	segmentCount := 128
	pool := NewBMTreePool(hasher, segmentCount, size)
	bmt := NewBMTHasher(pool)

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
	hashFunc = sha3.NewKeccak256

	t.ReportAllocs()
	t.ResetTimer()

	h := hashFunc()
	for i := 0; i < t.N; i++ {

		h.Reset()
		h.Write(data)
		//binary.Write(h, binary.LittleEndian, count)
		h.Sum(nil)

	}

}

func benchmarkSimpleBMT(n int, t *testing.B) {
	data := make([]byte, n)
	tdata := testDataReader(n)
	tdata.Read(data)
	hashFunc = sha3.NewKeccak256

	t.ReportAllocs()
	t.ResetTimer()

	//h := hashFunc()
	for i := 0; i < t.N; i++ {

		SimpleBMT(data, sha3.NewKeccak256)

	}

}
