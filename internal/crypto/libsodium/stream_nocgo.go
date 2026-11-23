//go:build !libsodium
// +build !libsodium

package libsodium

import "io"

func NewWriter(writer io.Writer, key []byte) io.WriteCloser {
	return newStreamWriter(writer, key)
}

func NewReader(reader io.Reader, key []byte) io.Reader {
	return newStreamReader(reader, key)
}
