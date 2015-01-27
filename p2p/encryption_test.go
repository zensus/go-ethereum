package p2p

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"testing"
)

func randomKey(i int) (key []byte) {
	key = make([]byte, i)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		panic(err)
	}
	return
}

func TestEncryption(t *testing.T) {
	var args [][]byte = make([][]byte, 4)
	for i, _ := range args {
		args[i] = randomKey(32)
	}
	var pubkey = randomKey(64)

	var caps []interface{}
	for _, p := range []Cap{Cap{"bzz", 0}, Cap{"shh", 1}, Cap{"eth", 2}} {
		caps = append(caps, p)
	}

	var msg0 = NewMsg(handshakeMsg,
		baseProtocolVersion,
		"ethersphere",
		caps,
		3301,
		pubkey,
	)

	var hs handshake

	conn0, conn1 := net.Pipe()
	rw0, err := NewSecureRW(conn0, conn0, args[0], args[1], args[2], args[3])
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	// note that args 3/2 swapped! ingress <-> egress MAC should reverse
	rw1, err := NewSecureRW(conn1, conn1, args[0], args[1], args[3], args[2])
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	go func() {
		if err = writeMsgToPacket(conn0, rw0, msg0); err != nil {
			t.Errorf("%v", err)
			return
		}
	}()

	var msg1 Msg
	if msg1, err = readMsgFromPacket(conn1, bufio.NewReader(rw1)); err != nil {
		t.Errorf("%v", err)
	}
	// Read(payload)

	if err = msg1.Decode(&hs); err != nil {
		t.Errorf("rlp decoding error: %v", err)
	}

	if //!bytes.Equal(hs.ListenPort, 3301) ||
	hs.ID != "ethersphere" ||
		len(hs.Caps) != 3 ||
		!bytes.Equal(hs.Pubkey(), pubkey) {
		t.Errorf("mismatch")
	}

}
