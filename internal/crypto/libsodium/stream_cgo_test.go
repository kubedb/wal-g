//go:build libsodium
// +build libsodium

package libsodium

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPureStreamMatchesCgo(t *testing.T) {
	key := testStreamKey(t)
	for _, n := range []int{1, 8191, 8192, 8193, 3 * 8192, 3*8192 + 1, 5<<20 + 123} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			plain := testPattern(n, n)
			var cgoOut bytes.Buffer
			w := NewWriter(&cgoOut, key)
			_, err := w.Write(plain)
			require.NoError(t, err)
			require.NoError(t, w.Close())

			got, err := decryptStream(key, cgoOut.Bytes(), 8192)
			require.NoError(t, err)
			assert.Equal(t, plain, got)

			withHeader(t, cgoOut.Bytes()[:streamHeaderBytes])
			pureOut := encryptStream(t, key, plain, 1000)
			assert.Equal(t, cgoOut.Bytes(), pureOut)

			back, err := io.ReadAll(NewReader(bytes.NewReader(pureOut), key))
			require.NoError(t, err)
			assert.Equal(t, plain, back)
		})
	}
}
