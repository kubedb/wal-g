package libsodium

import (
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/poly1305"
)

const (
	streamHeaderBytes  = 24
	streamABytes       = 1 + poly1305.TagSize
	streamCounterBytes = 4
	streamInonceBytes  = 8
	streamBlockBytes   = 64
	streamTagMessage   = 0x00
	streamTagPush      = 0x01
	streamTagRekey     = 0x02
	streamTagFinal     = streamTagPush | streamTagRekey
)

var (
	errStreamCorrupted = errors.New("corrupted chunk")
	errStreamTruncated = errors.Wrap(io.ErrUnexpectedEOF, "libsodium stream ended without the final chunk")
	streamPad0         [16]byte
)

type secretStream struct {
	k     [chacha20.KeySize]byte
	nonce [chacha20.NonceSize]byte
}

func newSecretStream(key, header []byte) (*secretStream, error) {
	if len(key) != chacha20.KeySize {
		return nil, fmt.Errorf("libsodium key must be %d bytes, got %d", chacha20.KeySize, len(key))
	}
	if len(header) != streamHeaderBytes {
		return nil, fmt.Errorf("libsodium header must be %d bytes, got %d", streamHeaderBytes, len(header))
	}
	subKey, err := chacha20.HChaCha20(key, header[:16])
	if err != nil {
		return nil, err
	}
	s := &secretStream{}
	copy(s.k[:], subKey)
	s.resetCounter()
	copy(s.nonce[streamCounterBytes:], header[16:])
	return s, nil
}

func (s *secretStream) resetCounter() {
	for i := 0; i < streamCounterBytes; i++ {
		s.nonce[i] = 0
	}
	s.nonce[0] = 1
}

func (s *secretStream) counterIsZero() bool {
	for i := 0; i < streamCounterBytes; i++ {
		if s.nonce[i] != 0 {
			return false
		}
	}
	return true
}

func (s *secretStream) cipher() *chacha20.Cipher {
	c, _ := chacha20.NewUnauthenticatedCipher(s.k[:], s.nonce[:])
	return c
}

func (s *secretStream) rekey() {
	var buf [chacha20.KeySize + streamInonceBytes]byte
	copy(buf[:chacha20.KeySize], s.k[:])
	copy(buf[chacha20.KeySize:], s.nonce[streamCounterBytes:])
	s.cipher().XORKeyStream(buf[:], buf[:])
	copy(s.k[:], buf[:chacha20.KeySize])
	copy(s.nonce[streamCounterBytes:], buf[chacha20.KeySize:])
	s.resetCounter()
}

func (s *secretStream) advance(mac []byte, tag byte) {
	for i := 0; i < streamInonceBytes; i++ {
		s.nonce[streamCounterBytes+i] ^= mac[i]
	}
	for i := 0; i < streamCounterBytes; i++ {
		s.nonce[i]++
		if s.nonce[i] != 0 {
			break
		}
	}
	if tag&streamTagRekey != 0 || s.counterIsZero() {
		s.rekey()
	}
}

func (s *secretStream) start() (*chacha20.Cipher, *poly1305.MAC) {
	c := s.cipher()
	var block [streamBlockBytes]byte
	c.XORKeyStream(block[:], block[:])
	var polyKey [32]byte
	copy(polyKey[:], block[:32])
	return c, poly1305.New(&polyKey)
}

func streamAuthenticate(mac *poly1305.MAC, block, ciphertext []byte) [poly1305.TagSize]byte {
	_, _ = mac.Write(block)
	_, _ = mac.Write(ciphertext)
	_, _ = mac.Write(streamPad0[:(0x10-streamBlockBytes+len(ciphertext))&0xf])
	var lengths [16]byte
	binary.LittleEndian.PutUint64(lengths[8:], uint64(streamBlockBytes+len(ciphertext)))
	_, _ = mac.Write(lengths[:])
	var tag [poly1305.TagSize]byte
	mac.Sum(tag[:0])
	return tag
}

func (s *secretStream) push(dst, m []byte, tag byte) []byte {
	c, mac := s.start()
	block := [streamBlockBytes]byte{tag}
	c.XORKeyStream(block[:], block[:])
	out, chunk := sliceForAppend(dst, streamABytes+len(m))
	chunk[0] = block[0]
	ciphertext := chunk[1 : 1+len(m)]
	c.XORKeyStream(ciphertext, m)
	sum := streamAuthenticate(mac, block[:], ciphertext)
	copy(chunk[1+len(m):], sum[:])
	s.advance(sum[:], tag)
	return out
}

func (s *secretStream) pull(dst, in []byte) ([]byte, byte, error) {
	if len(in) < streamABytes {
		return nil, 0, errStreamCorrupted
	}
	mlen := len(in) - streamABytes
	c, mac := s.start()
	block := [streamBlockBytes]byte{in[0]}
	c.XORKeyStream(block[:], block[:])
	tag := block[0]
	block[0] = in[0]
	ciphertext := in[1 : 1+mlen]
	sum := streamAuthenticate(mac, block[:], ciphertext)
	if subtle.ConstantTimeCompare(sum[:], in[1+mlen:]) != 1 {
		return nil, 0, errStreamCorrupted
	}
	out, m := sliceForAppend(dst, mlen)
	c.XORKeyStream(m, ciphertext)
	s.advance(sum[:], tag)
	return out, tag, nil
}

func sliceForAppend(in []byte, n int) (head, tail []byte) {
	if total := len(in) + n; cap(in) >= total {
		head = in[:total]
	} else {
		head = make([]byte, total)
		copy(head, in)
	}
	tail = head[len(in):]
	return head, tail
}
