package libsodium

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testStreamKeyHex = "4c0829fdfe7ae1987918edc585b1a90556d901eaea963c7625bb5734576dfb59"

type katChunk struct {
	Tag        byte   `json:"tag"`
	Plaintext  string `json:"plaintext"`
	Ciphertext string `json:"ciphertext"`
}

type katStream struct {
	Key     string     `json:"key"`
	Header  string     `json:"header"`
	Counter string     `json:"counter"`
	Chunks  []katChunk `json:"chunks"`
}

func mustHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func testPattern(n, seed int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*31 + seed) % 256)
	}
	return b
}

func testStreamKey(t *testing.T) []byte {
	return mustHex(t, testStreamKeyHex)
}

func encryptStream(t *testing.T, key, plain []byte, writeSize int) []byte {
	var buf bytes.Buffer
	w := newStreamWriter(&buf, key)
	for off := 0; off < len(plain); off += writeSize {
		end := min(off+writeSize, len(plain))
		n, err := w.Write(plain[off:end])
		require.NoError(t, err)
		require.Equal(t, end-off, n)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func decryptStream(key, stream []byte, readSize int) ([]byte, error) {
	r := newStreamReader(bytes.NewReader(stream), key)
	out := bytes.NewBuffer([]byte{})
	buf := make([]byte, readSize)
	for {
		n, err := r.Read(buf)
		out.Write(buf[:n])
		if err == io.EOF {
			return out.Bytes(), nil
		}
		if err != nil {
			return out.Bytes(), err
		}
	}
}

func expectedStreamSize(n int) int {
	return streamHeaderBytes + n/chunkSize*(chunkSize+streamABytes) + n%chunkSize + streamABytes
}

func withHeader(t *testing.T, header []byte) {
	saved := streamRandom
	streamRandom = bytes.NewReader(header)
	t.Cleanup(func() { streamRandom = saved })
}

func TestSecretStreamKnownAnswers(t *testing.T) {
	data, err := os.ReadFile("testdata/secretstream_kat.json")
	require.NoError(t, err)
	var fixture map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fixture))
	for _, name := range []string{"stream", "wrap"} {
		t.Run(name, func(t *testing.T) {
			var kat katStream
			require.NoError(t, json.Unmarshal(fixture[name], &kat))
			key, header := mustHex(t, kat.Key), mustHex(t, kat.Header)
			push, err := newSecretStream(key, header)
			require.NoError(t, err)
			pull, err := newSecretStream(key, header)
			require.NoError(t, err)
			if kat.Counter != "" {
				copy(push.nonce[:streamCounterBytes], mustHex(t, kat.Counter))
				copy(pull.nonce[:streamCounterBytes], mustHex(t, kat.Counter))
			}
			for i, c := range kat.Chunks {
				plain, cipher := mustHex(t, c.Plaintext), mustHex(t, c.Ciphertext)
				assert.Equal(t, c.Ciphertext, hex.EncodeToString(push.push(nil, plain, c.Tag)), "push %d", i)
				got, tag, err := pull.pull(nil, cipher)
				require.NoError(t, err, "pull %d", i)
				assert.Equal(t, c.Plaintext, hex.EncodeToString(got), "pull %d", i)
				assert.Equal(t, c.Tag, tag, "pull %d", i)
			}
		})
	}
}

func TestStreamWriterMatchesLibsodiumFraming(t *testing.T) {
	key := testStreamKey(t)
	for _, n := range []int{8192, 8193} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			fixture, err := os.ReadFile(fmt.Sprintf("testdata/walg_cgo_%d.bin", n))
			require.NoError(t, err)
			plain := testPattern(n, 7)
			got, err := decryptStream(key, fixture, 4096)
			require.NoError(t, err)
			assert.Equal(t, plain, got)
			withHeader(t, fixture[:streamHeaderBytes])
			assert.Equal(t, fixture, encryptStream(t, key, plain, len(plain)))
		})
	}
}

func TestStreamRoundTrip(t *testing.T) {
	key := testStreamKey(t)
	for _, n := range []int{0, 1, 8191, 8192, 8193, 3 * 8192, 3*8192 + 1, 5<<20 + 123} {
		plain := testPattern(n, n)
		for _, writeSize := range []int{1, 1000, 8192, 10000, max(n, 1)} {
			if writeSize == 1 && n > 3*8192+1 {
				continue
			}
			t.Run(fmt.Sprintf("size=%d/write=%d", n, writeSize), func(t *testing.T) {
				stream := encryptStream(t, key, plain, writeSize)
				assert.Equal(t, expectedStreamSize(n), len(stream))
				for _, readSize := range []int{7, 8192, 1 << 20} {
					got, err := decryptStream(key, stream, readSize)
					require.NoError(t, err)
					assert.Equal(t, plain, got)
				}
			})
		}
	}
}

func TestStreamReaderKeepsReturningEOF(t *testing.T) {
	key := testStreamKey(t)
	r := newStreamReader(bytes.NewReader(encryptStream(t, key, testPattern(100, 1), 100)), key)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Len(t, got, 100)
	for i := 0; i < 3; i++ {
		n, err := r.Read(make([]byte, 8))
		assert.Equal(t, 0, n)
		assert.Equal(t, io.EOF, err)
	}
}

func TestStreamReaderDetectsTruncation(t *testing.T) {
	key := testStreamKey(t)
	for _, n := range []int{0, 1, 8192, 8193} {
		stream := encryptStream(t, key, testPattern(n, 3), 4096)
		for cut := 0; cut < len(stream); cut++ {
			_, err := decryptStream(key, stream[:cut], 8192)
			require.Error(t, err, "size %d cut at %d of %d", n, cut, len(stream))
		}
	}
}

func TestStreamReaderDetectsTampering(t *testing.T) {
	key := testStreamKey(t)
	plain := testPattern(8193, 5)
	stream := encryptStream(t, key, plain, len(plain))
	for i := range stream {
		tampered := bytes.Clone(stream)
		tampered[i] ^= 0x80
		_, err := decryptStream(key, tampered, 8192)
		require.Error(t, err, "flipped byte %d", i)
	}
	_, err := decryptStream(key, append(bytes.Clone(stream), 0), 8192)
	require.Error(t, err, "data after the final chunk")
	other := bytes.Clone(key)
	other[0] ^= 1
	_, err = decryptStream(other, stream, 8192)
	require.Error(t, err, "wrong key")
}

func TestStreamRejectsBadKeyLength(t *testing.T) {
	_, err := newStreamWriter(io.Discard, make([]byte, 31)).Write([]byte("x"))
	require.Error(t, err)
	_, err = decryptStream(make([]byte, 31), make([]byte, 64), 8)
	require.Error(t, err)
}

func TestCrypterRoundTripsWithEveryKeyTransform(t *testing.T) {
	plain := testPattern(20000, 9)
	for _, tc := range []struct{ key, transform string }{
		{"TEST_LIBSODIUM_KEY_______", KeyTransformNone},
		{"0123456789abcdef0123456789abcdef_trimmed", KeyTransformNone},
		{testStreamKeyHex, KeyTransformHex},
		{"jv81yb3v3gNePrY0JmJ4q2j2NrqcM7tDYSHFoZ0tTIw=", KeyTransformBase64},
	} {
		crypter := CrypterFromKey(tc.key, tc.transform)
		var buf bytes.Buffer
		w, err := crypter.Encrypt(&buf)
		require.NoError(t, err)
		_, err = w.Write(plain)
		require.NoError(t, err)
		require.NoError(t, w.Close())
		r, err := crypter.Decrypt(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, plain, got, tc.transform)
	}
}
