// This implementation does not take advantage of any paralellisms and uses
// far more memory than necessary, but it is easy to see that it is correct.
// It can be used for generating test cases for optimized implementations.
package storage

import (
// "fmt"
)

type simpleBMT struct {
	datasection int
	hasher      Hasher
	chunk       []byte
	length      int
}

func SimpleBMT(chunk []byte, hasher Hasher) []byte {
	datasection := 2 * hasher().Size()
	length := len(chunk)
	var section int
	for section = datasection; section < length; section *= 2 {
	}
	bmt := &simpleBMT{
		datasection: datasection,
		hasher:      hasher,
		chunk:       chunk,
		length:      length,
	}
	return bmt.hash(0, section)
}

func (self *simpleBMT) hash(offset, section int) []byte {
	chunk := self.chunk
	if section <= self.datasection {
		end := offset + self.datasection
		if end > self.length {
			end = self.length
		}
		h := self.hasher()
		h.Write(chunk[offset:end])
		hash := h.Sum(nil)
		// fmt.Printf("data section %v-%v hash: %x\n (data: %x)\n", offset, end, hash, chunk[offset:end])
		return hash
	}
	section /= 2
	if self.length-offset > section {
		h := self.hasher()
		left := self.hash(offset, section)
		right := self.hash(offset+section, section)
		h.Write(left)
		h.Write(right)
		hash := h.Sum(nil)
		// fmt.Printf("intermediate section %v (max %v): %x\n (data: %x|%x)\n", offset, 2*section, hash, left, right)
		return hash
	}
	return self.hash(offset, section)
}
