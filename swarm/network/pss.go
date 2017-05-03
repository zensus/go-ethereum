package network

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/pot"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

const (
	DefaultTTL                  = 6000
	TopicLength                 = 32
	TopicResolverLength         = 8
	PssPeerCapacity             = 256
	PssPeerTopicDefaultCapacity = 8
	digestLength                = 64
	digestCapacity              = 256
	defaultDigestCacheTTL       = time.Second
	pingTopicName               = "pss"
	pingTopicVersion            = 1
	pssCodeOffset               = 20
)

var (
	errorNoForwarder   = errors.New("no available forwarders in routing table")
	errorForwardToSelf = errors.New("forward to self")
	errorBlockByCache  = errors.New("message found in blocking cache")
)

// Defines params for Pss
type PssParams struct {
	Cachettl time.Duration
}

// Initializes default params for Pss
func NewPssParams() *PssParams {
	return &PssParams{
		Cachettl: defaultDigestCacheTTL,
	}
}

// Encapsulates the message transported over pss.
type PssMsg struct {
	To      []byte
	Payload *PssEnvelope
}

// String representation of PssMsg
func (self *PssMsg) String() string {
	return fmt.Sprintf("PssMsg: Recipient: %x", common.ByteLabel(self.To))
}

// Topic defines the context of a message being transported over pss
// It is used by pss to determine what action is to be taken on an incoming message
// Typically, one can map protocol handlers for the message payloads by mapping topic to them; see *Pss.Register()
type PssTopic [TopicLength]byte

// Pre-Whisper placeholder, payload of PssMsg
type PssEnvelope struct {
	Topic       PssTopic
	TTL         uint16
	Payload     []byte
	SenderOAddr []byte
	SenderUAddr []byte
}

// creates Pss envelope from sender address, topic and raw payload
func NewPssEnvelope(addr PeerAddr, topic PssTopic, payload []byte) *PssEnvelope {
	return &pssEnvelope{
		SenderOAddr: addr.OverlayAddr(),
		SenderUAddr: addr.UnderlayAddr(),
		Topic:       topic,
		TTL:         DefaultTTL,
		Payload:     payload,
	}
}

// encapsulates a protocol msg as PssEnvelope data
type PssProtocolMsg struct {
	Code       uint64
	Size       uint32
	Data       []byte
	ReceivedAt time.Time
}

type pssCacheEntry struct {
	expiresAt    time.Time
	receivedFrom []byte
}

type pssDigest uint32

// Message handler func for a topic
type pssHandler func(msg []byte, p *p2p.Peer, from []byte) error

// pss provides sending messages to nodes without having to be directly connected to them.
//
// The messages are wrapped in a PssMsg structure and routed using the swarm kademlia routing.
//
// The top-level Pss object provides:
//
// - access to the swarm overlay and routing (kademlia)
// - a collection of remote overlay addresses mapped to MsgReadWriters, representing the virtually connected peers
// - a collection of remote underlay address, mapped to the overlay addresses above
// - a method to send a message to specific overlayaddr
// - a dispatcher lookup, mapping protocols to topics
// - a message cache to spot messages that previously have been forwarded
type Pss struct {
	Overlay                                                 // we can get the overlayaddress from this
	peerPool map[pot.Address]map[PssTopic]p2p.MsgReadWriter // keep track of all virtual p2p.Peers we are currently speaking to
	handlers map[PssTopic]map[*pssHandler]bool              // topic and version based pss payload handlers
	fwdcache map[pssDigest]pssCacheEntry                    // checksum of unique fields from pssmsg mapped to expiry, cache to determine whether to drop msg
	cachettl time.Duration                                  // how long to keep messages in fwdcache
	hasher   func(string) storage.Hasher                    // hasher to digest message to cache
	lock     sync.Mutex
}

func (self *Pss) hashMsg(msg *PssMsg) pssDigest {
	hasher := self.hasher("SHA3")()
	hasher.Reset()
	hasher.Write(msg.To)
	hasher.Write(msg.Payload.SenderUAddr)
	hasher.Write(msg.Payload.SenderOAddr)
	hasher.Write(msg.Payload.Topic[:])
	hasher.Write(msg.Payload.Payload)
	b := hasher.Sum([]byte{})
	return pssDigest(binary.BigEndian.Uint32(b))
}

// Creates a new Pss instance. A node should only need one of these
//
// TODO: error check overlay integrity
func NewPss(k Overlay, params *PssParams) *Pss {
	return &Pss{
		Overlay: k,
		//peerPool: make(map[pot.Address]map[PssTopic]*PssReadWriter, PssPeerCapacity),
		peerPool: make(map[pot.Address]map[PssTopic]p2p.MsgReadWriter, PssPeerCapacity),
		handlers: make(map[PssTopic]map[*pssHandler]bool),
		fwdcache: make(map[pssDigest]pssCacheEntry),
		cachettl: params.Cachettl,
		hasher:   storage.MakeHashFunc,
	}
}

// implements the ProtocolService interface
func (self *Pss) RegisterMsgTypes(ct *protocols.CodeMap) {
	ct.Register(pssCodeOffset, &PssMsg{})
}

// the protocol module service to run on a peer
// implements the ProtocolService interface
func (self *Pss) ProtocolRun(p interface{}) error {
	p.(Peer).Register(&PssMsg{}, self.handlePssMsg)
	return nil
}

// Takes the generated PssTopic of a protocol/chatroom etc, and links a handler function to it
// This allows the implementer to retrieve the right handler functions (invoke the right protocol)
// for an incoming message by inspecting the topic on it.
// a topic allows for multiple handlers
// returns a deregister function which needs to be called to deregister the handler
// (similar to event.Subscription.Unsubscribe())
func (self *Pss) Register(topic PssTopic, handler pssHandler) func() {
	self.lock.Lock()
	defer self.lock.Unlock()
	handlers := self.handlers[topic]
	if handlers == nil {
		handlers = make(map[*pssHandler]bool)
		self.handlers[topic] = handlers
	}
	handlers[&handler] = true
	return func() { self.deregister(topic, &handler) }
}

func (self *Pss) deregister(topic PssTopic, h *pssHandler) {
	self.lock.Lock()
	defer self.lock.Unlock()
	handlers := self.handlers[topic]
	if len(handlers) == 1 {
		delete(self.handlers, topic)
		return
	}
	delete(handlers, h)
}

func (self *Pss) addFwdCacheSender(addr []byte, digest pssDigest) error {
	self.lock.Lock()
	defer self.lock.Unlock()
	var entry pssCacheEntry
	var ok bool
	if entry, ok = self.fwdcache[digest]; !ok {
		entry = pssCacheEntry{}
	}
	entry.receivedFrom = addr
	self.fwdcache[digest] = entry
	return nil
}

func (self *Pss) addFwdCacheExpire(digest pssDigest) error {
	self.lock.Lock()
	defer self.lock.Unlock()
	var entry pssCacheEntry
	var ok bool
	if entry, ok = self.fwdcache[digest]; !ok {
		entry = pssCacheEntry{}
	}
	entry.expiresAt = time.Now().Add(self.cachettl)
	self.fwdcache[digest] = entry
	return nil
}

func (self *Pss) checkFwdCache(addr []byte, digest pssDigest) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	entry, ok := self.fwdcache[digest]
	if ok {
		if entry.expiresAt.After(time.Now()) {
			log.Debug(fmt.Sprintf("unexpired cache for digest %x", digest))
			return true
		} else if entry.expiresAt.IsZero() && bytes.Equal(addr, entry.receivedFrom) {
			log.Debug(fmt.Sprintf("sendermatch %x for digest %x", common.ByteLabel(addr), digest))
			return true
		}
	}
	return false
}

func (self *Pss) API() *rpc.API {
	return &rpc.API{
		Namespace: "eth",
		Version:   "0.1/pss",
		Service:   NewPssAPI(self),
		Public:    true,
	}
}

func (self *Pss) getHandlers(topic PssTopic) map[*pssHandler]bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.handlers[topic]
}

//
func (self *Pss) handlePssMsg(msg interface{}) error {
	pssmsg := msg.(*PssMsg)

	if !self.isSelfRecipient(pssmsg) {
		log.Trace("pss was for someone else :'( ... forwarding")
		return self.Forward(pssmsg)
	}
	log.Trace("pss for us, yay! ... let's process!")
	return self.Process(pssmsg)
}

// processes a message with self as recipient
func (self *Pss) Process(pssmsg *PssMsg) error {
	env := pssmsg.Payload
	payload := env.Payload
	handlers := self.getHandlers(env.Topic)
	if len(handlers) == 0 {
		return fmt.Errorf("No registered handler for topic '%s'", env.Topic)
	}
	nid := adapters.NewNodeId(env.SenderUAddr)
	p := p2p.NewPeer(nid.NodeID, fmt.Sprintf("%x", common.ByteLabel(nid.Bytes())), []p2p.Cap{})
	for f := range handlers {
		err := (*f)(payload, p, env.SenderOAddr)
		if err != nil {
			return err
		}
	}
	return nil
}

// Sends a message using pss. The message could be anything at all, and will be handled by whichever handler function is mapped to PssTopic using *Pss.Register()
//
// The to address is a swarm overlay address
func (self *Pss) Send(to []byte, topic PssTopic, msg []byte) error {
	sender := self.Overlay.GetAddr()
	pssenv := NewPssEnvelope(sender, topic, msg)
	pssmsg := &PssMsg{
		To:      to,
		Payload: pssenv,
	}
	return self.Forward(pssmsg)
}

// Forwards a pss message to the peer(s) closest to the to address
//
// Handlers that want to pass on a message should call this directly
func (self *Pss) Forward(msg *PssMsg) error {

	if self.isSelfRecipient(msg) {
		return errorForwardToSelf
	}

	digest := self.hashMsg(msg)

	if self.checkFwdCache(nil, digest) {
		log.Trace(fmt.Sprintf("QROM %x TO %x", common.ByteLabel(self.Overlay.GetAddr().OverlayAddr()), common.ByteLabel(msg.To)))
		return nil
	}

	// TODO:check integrity of message
	sent := 0

	// send with kademlia
	// find the closest peer to the recipient and attempt to send
	self.Overlay.EachLivePeer(msg.To, 256, func(p Peer, po int, isproxbin bool) bool {
		addr := self.Overlay.GetAddr()
		sendMsg := fmt.Sprintf("%x: msg to %x via %x", common.ByteLabel(addr.OverlayAddr()), common.ByteLabel(msg.To), common.ByteLabel(p.OverlayAddr()))
		if self.checkFwdCache(p.OverlayAddr(), digest) {
			log.Info(fmt.Sprintf("%v: peer already forwarded to", sendMsg))
			return true
		}
		err := p.Send(msg)
		if err != nil {
			log.Warn(fmt.Sprintf("%v: failed forwarding: %v", sendMsg, err))
			return true
		}
		log.Trace(fmt.Sprintf("%v: successfully forwarded", sendMsg))
		sent++
		// if equality holds, p is always the first peer given in the iterator
		if bytes.Equal(msg.To, p.OverlayAddr()) || !isproxbin {
			return false
		}
		log.Trace(fmt.Sprintf("%x is in proxbin, keep forwarding", common.ByteLabel(p.OverlayAddr())))
		return true
	})

	if sent == 0 {
		return fmt.Errorf("PSS: unable to forward to any peers")
	}

	self.addFwdCacheExpire(digest)
	return nil
}

// Links a pss peer address and topic to a dedicated p2p.MsgReadWriter in the pss peerpool, and runs the specificed protocol on this p2p.MsgReadWriter and the specified peer
//
// The effect is that now we have a "virtual" protocol running on an artificial p2p.Peer, which can be looked up and piped to through Pss using swarm overlay address and topic
func (self *Pss) AddPeer(p *p2p.Peer, addr pot.Address, protocall adapters.ProtoCall, topic PssTopic, rw p2p.MsgReadWriter) error {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.addPeerTopic(addr, topic, rw)
	go func() {
		err := protocall(p, rw)
		log.Warn(fmt.Sprintf("pss vprotocol quit on addr %v topic %v: %v", addr, topic, err))
		self.removePeerTopic(rw.To, topic)
	}()
	return nil
}

// Removes a pss peer from the pss peerpool
func (self *Pss) RemovePeer(id pot.Address) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.peerPool[id] = nil
	return
}

func (self *Pss) addPeerTopic(id pot.Address, topic PssTopic, rw p2p.MsgReadWriter) error {
	if self.peerPool[id][topic] == nil {
		self.peerPool[id] = make(map[PssTopic]p2p.MsgReadWriter, PssPeerTopicDefaultCapacity)
	}
	self.peerPool[id][topic] = rw
	return nil
}

func (self *Pss) removePeerTopic(id pot.Address, topic PssTopic) {
	delete(self.peerPool[id], topic)
}

func (self *Pss) isActive(id pot.Address, topic PssTopic) bool {
	return self.peerPool[id][topic] != nil
}

// Convenience object that:
//
// - allows passing of the unwrapped PssMsg payload to the p2p level message handlers
// - interprets outgoing p2p.Msg from the p2p level to pass in to *Pss.Send()
//
// Implements p2p.MsgReadWriter
type PssReadWriter struct {
	*Pss
	To         pot.Address
	LastActive time.Time
	rw         chan p2p.Msg
	ct         *protocols.CodeMap
	topic      *PssTopic
}

// Implements p2p.MsgReader
func (prw PssReadWriter) ReadMsg() (p2p.Msg, error) {
	msg := <-prw.rw
	log.Trace(fmt.Sprintf("pssrw readmsg: %v", msg))
	return msg, nil
}

// Implements p2p.MsgWriter
func (prw PssReadWriter) WriteMsg(msg p2p.Msg) error {
	log.Trace(fmt.Sprintf("pssrw writemsg: %v", msg))
	ifc, found := prw.ct.GetInterface(msg.Code)
	if !found {
		return fmt.Errorf("Writemsg couldn't find matching interface for code %d", msg.Code)
	}
	msg.Decode(ifc)

	pmsg, err := newProtocolMessage(msg.Code, ifc)
	if err != nil {
		return err
	}
	return prw.Pss.Send(prw.To.Bytes(), *prw.topic, pmsg)
}

// Injects a p2p.Msg into the MsgReadWriter, so that it appears on the associated p2p.MsgReader
func (prw PssReadWriter) injectMsg(msg p2p.Msg) error {
	log.Trace(fmt.Sprintf("pssrw injectmsg: %v", msg))
	prw.rw <- msg
	return nil
}

// Convenience object for passing messages in and out of the p2p layer
type PssProtocol struct {
	*Pss
	proto *p2p.Protocol
	topic *PssTopic
	ct    *protocols.CodeMap
}

// Constructor
func NewPssProtocol(pss *Pss, topic *PssTopic, ct *protocols.CodeMap, proto *p2p.Protocol) *PssProtocol {
	return &PssProtocol{
		Pss:   pss,
		proto: proto,
		topic: topic,
		ct:    ct,
	}
}

func (self *PssProtocol) handle(msg []byte, p *p2p.Peer, senderAddr []byte) error {
	hashoaddr := pot.NewHashAddressFromBytes(senderAddr).Address
	if !self.isActive(hashoaddr, *self.topic) {
		rw := &PssReadWriter{
			Pss:   self.Pss,
			To:    hashoaddr,
			rw:    make(chan p2p.Msg),
			ct:    self.ct,
			topic: self.topic,
		}
		self.Pss.AddPeer(p, hashoaddr, self.proto.Run, *self.topic, rw)
	}

	payload := &pssPayload{}
	if err := rlp.DecodeBytes(msg, payload); err != nil {
		return fmt.Errorf("pss protocol handler unable to decode payload as p2p message: %v", err)
	}

	pmsg := p2p.Msg{
		Code:       payload.Code,
		Size:       uint32(len(payload.Data)),
		ReceivedAt: time.Now(),
		Payload:    bytes.NewBuffer(payload.Data),
	}

	vrw := self.Pss.peerPool[hashoaddr][*self.topic].(*PssReadWriter)
	vrw.injectMsg(pmsg)

	return nil
}

func (self *Pss) isSelfRecipient(msg *PssMsg) bool {
	return bytes.Equal(msg.To, self.Overlay.GetAddr().OverlayAddr())
}

func newProtocolMsg(code uint64, msg interface{}) ([]byte, error) {

	rlpdata, err := rlp.EncodeToBytes(msg)
	if err != nil {
		return nil, err
	}

	// previous attempts corrupted nested structs in the payload iself upon deserializing
	// therefore we use two separate []byte fields instead of peerAddr
	// TODO verify that nested structs cannot be used in rlp
	smsg := &pssPayload{
		Code: code,
		Size: uint32(len(rlpdata)),
		Data: rlpdata,
	}

	return rlp.EncodeToBytes(smsg)
}

// constructs a new PssTopic from a given name and version.
//
// Analogous to the name and version members of p2p.Protocol
func NewTopic(s string, v int) PssTopic {
	h := sha3.NewKeccak()
	h.Write([]byte(s))
	buf := make([]byte, 8)
	bint, _ := binary.PutUvarint(buf, int64(v))
	h.Write(bint)
	return h.Sum()
}
