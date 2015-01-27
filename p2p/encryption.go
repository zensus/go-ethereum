package p2p

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"hash"
	"io"
)

var blockSize = aes.BlockSize

// secureRW implements a rlp.ByteReader as well as an io.Writer
// and read writer with encryption and authentication
// it is initialised by Run() after a successful crypto handshake
// aesSecret, macSecret, egressMac, ingress
type secureRW struct {
	aesSecret, macSecret, egressMac, ingressMac []byte
	r                                           io.Reader
	w                                           io.Writer
	ingress, egress                             hash.Hash
	block                                       cipher.Block
}

func NewSecureRW(r io.Reader, w io.Writer, aesSecret, macSecret, egressMac, ingressMac []byte) (rw *secureRW, err error) {
	rw = &secureRW{
		r:          r,
		w:          w,
		aesSecret:  aesSecret,
		macSecret:  macSecret,
		egressMac:  egressMac,
		ingressMac: ingressMac,
	}
	rw.block, err = aes.NewCipher(aesSecret)
	if err != nil {
		return nil, err
	}
	rw.egress = hmac.New(sha256.New, egressMac)
	rw.ingress = hmac.New(sha256.New, ingressMac)
	return rw, nil
}

// ciphertext := iv(blockSize) || ciphertext(len(plaintext))
func (self *secureRW) Decrypt(plaintext, ciphertext []byte) (err error) {
	ctr := cipher.NewCTR(self.block, ciphertext[:blockSize])
	ctr.XORKeyStream(plaintext, ciphertext[blockSize:])
	self.ingress.Write(plaintext)
	return
}

// len(ciphertext) >= blockSize + len(plaintext) + shaLen
func (self *secureRW) Encrypt(ciphertext, plaintext []byte) (err error) {
	if _, err = rand.Read(ciphertext[:blockSize]); err == nil {
		ctr := cipher.NewCTR(self.block, ciphertext[:blockSize])
		ctr.XORKeyStream(ciphertext[blockSize:blockSize+len(plaintext)], plaintext)
	}
	return
}

func (self *secureRW) Write(plaintext []byte) (n int, err error) {
	ciphertext := make([]byte, blockSize+len(plaintext)+shaLen)
	self.Encrypt(ciphertext, plaintext)
	self.egress.Write(plaintext)
	copy(ciphertext[blockSize+len(plaintext):len(ciphertext)], self.egress.Sum(nil)) // keyed hash
	return self.w.Write(ciphertext)
}

// len(ciphertext) >= blockSize + len(payload) + shaLen
func (self *secureRW) Read(payload []byte) (n int, err error) {
	ciphertext := make([]byte, blockSize+len(payload)) //sync.Pool?
	if n, err = self.r.Read(ciphertext[:blockSize+len(payload)]); err != nil {
		return
	}
	self.Decrypt(payload, ciphertext[:blockSize+len(payload)])
	mac := make([]byte, shaLen) // sync.Pool
	if _, err = self.r.Read(mac); err != nil {
		err = newPeerError(errRead, "%v", err)
		return
	}
	self.ingress.Write(payload)
	var expectedMac = self.ingress.Sum(nil)
	if !hmac.Equal(expectedMac, mac) {
		err = newPeerError(errAuthentication, "ingress MAC incorrect")
		return
	}
	return
}
