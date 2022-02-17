//go:build lzo
// +build lzo

package test

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"testing"
)

// Test extraction of various lzo compressed tar files.
func testLzopRoundTrip(t *testing.T, stride, nBytes int) {
	// Generate and save random bytes compare against
	// compression-decompression cycle.
	sb := tools.NewStrideByteReader(stride)
	lr := &io.LimitedReader{
		R: sb,
		N: int64(nBytes),
	}
	b, err := ioutil.ReadAll(lr)

	// Copy generated bytes to another slice to make the test more
	// robust against modifications of "b".
	bCopy := make([]byte, len(b))
	copy(bCopy, b)
	if err != nil {
		t.Log(err)
	}

	// Compress bytes and make a tar in memory.
	r, w := io.Pipe()
	lzow := internal.NewLzoWriter(w)
	go func() {
		defer lzow.Close()
		defer w.Close()
		bw := bufio.NewWriterSize(lzow, internal.LzopBlockSize)
		defer func() {
			if err := bw.Flush(); err != nil {
				panic(err)
			}
		}()

		tools.CreateTar(bw, &io.LimitedReader{
			R: bytes.NewBuffer(b),
			N: int64(len(b)),
		})

	}()
	tarContents := &bytes.Buffer{}
	io.Copy(tarContents, r)

	// Extract the generated tar and check that its one member is
	// the same as the bytes generated to begin with.
	brm := &BufferReaderMaker{tarContents, "/usr/local", "lzo"}
	buf := &tools.BufferTarInterpreter{}
	files := []internal.ReaderMaker{brm}
	err = internal.ExtractAll(buf, files)
	if err != nil {
		t.Log(err)
	}

	assert.Equalf(t, bCopy, buf.Out, "extract: Decompressed output does not match input.")
}

func TestLzopUncompressableBytes(t *testing.T) {
	testLzopRoundTrip(t, internal.LzopBlockSize*2, internal.LzopBlockSize*2)
}
func TestLzop1Byte(t *testing.T)   { testLzopRoundTrip(t, 7924, 1) }
func TestLzop1MByte(t *testing.T)  { testLzopRoundTrip(t, 7924, 1024*1024) }
func TestLzop10MByte(t *testing.T) { testLzopRoundTrip(t, 7924, 10*1024*1024) }

func setupRand(stride, nBytes int) *BufferReaderMaker {
	sb := tools.NewStrideByteReader(stride)
	lr := &io.LimitedReader{
		R: sb,
		N: int64(nBytes),
	}
	b := &BufferReaderMaker{&bytes.Buffer{}, "/usr/local", "lzo"}

	pr, pw := io.Pipe()
	lzow := internal.NewLzoWriter(pw)

	go func() {
		tools.CreateTar(lzow, lr)
		defer lzow.Close()
		defer pw.Close()
	}()

	_, err := io.Copy(b.Buf, pr)

	if err != nil {
		panic(err)
	}

	return b
}

func BenchmarkExtractAll(b *testing.B) {
	b.SetBytes(int64(b.N * 1024 * 1024))
	out := make([]internal.ReaderMaker, 1)
	rand := setupRand(7924, b.N*1024*1024)
	fmt.Println("B.N", b.N)

	out[0] = rand

	b.ResetTimer()

	// f := &extract.FileTarInterpreter{
	// 		DBDataDirectory: "",
	// 	}
	// out[0] = f

	// extract.ExtractAll(f, out)

	// np := &extract.NOPTarInterpreter{}
	// extract.ExtractAll(np, out)

	buf := &tools.BufferTarInterpreter{}
	err := internal.ExtractAll(buf, out)
	if err != nil {
		b.Log(err)
	}

}
