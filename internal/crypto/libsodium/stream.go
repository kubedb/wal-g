package libsodium

import (
	"crypto/rand"
	"io"

	"github.com/pkg/errors"
)

var streamRandom io.Reader = rand.Reader

type streamWriter struct {
	writer  io.Writer
	key     []byte
	state   *secretStream
	in      []byte
	out     []byte
	started bool
	closed  bool
	err     error
}

func newStreamWriter(writer io.Writer, key []byte) *streamWriter {
	return &streamWriter{
		writer: writer,
		key:    key,
		in:     make([]byte, 0, chunkSize),
		out:    make([]byte, 0, chunkSize+streamABytes),
	}
}

func (w *streamWriter) start() error {
	if w.started {
		return w.err
	}
	w.started = true
	header := make([]byte, streamHeaderBytes)
	if _, err := io.ReadFull(streamRandom, header); err != nil {
		w.err = errors.Wrap(err, "failed to generate libsodium header")
		return w.err
	}
	state, err := newSecretStream(w.key, header)
	if err != nil {
		w.err = err
		return w.err
	}
	w.state = state
	if _, err := w.writer.Write(header); err != nil {
		w.err = errors.Wrap(err, "failed to write libsodium header")
		return w.err
	}
	return nil
}

func (w *streamWriter) Write(p []byte) (int, error) {
	if err := w.start(); err != nil {
		return 0, err
	}
	n := 0
	for n < len(p) {
		count := copy(w.in[len(w.in):cap(w.in)], p[n:])
		w.in = w.in[:len(w.in)+count]
		n += count
		if len(w.in) == cap(w.in) {
			if err := w.flush(streamTagMessage); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

func (w *streamWriter) flush(tag byte) error {
	w.out = w.state.push(w.out[:0], w.in, tag)
	w.in = w.in[:0]
	if _, err := w.writer.Write(w.out); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *streamWriter) Close() error {
	if w.closed {
		return w.err
	}
	w.closed = true
	if closer, ok := w.writer.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}
	if err := w.start(); err != nil {
		return err
	}
	return w.flush(streamTagFinal)
}

type streamReader struct {
	reader  io.Reader
	key     []byte
	state   *secretStream
	in      []byte
	out     []byte
	pos     int
	started bool
	final   bool
	err     error
}

func newStreamReader(reader io.Reader, key []byte) *streamReader {
	return &streamReader{
		reader: reader,
		key:    key,
		in:     make([]byte, chunkSize+streamABytes),
		out:    make([]byte, 0, chunkSize),
	}
}

func (r *streamReader) start() {
	r.started = true
	header := make([]byte, streamHeaderBytes)
	if _, err := io.ReadFull(r.reader, header); err != nil {
		r.err = errors.Wrap(err, "failed to read libsodium header")
		return
	}
	r.state, r.err = newSecretStream(r.key, header)
}

func (r *streamReader) Read(p []byte) (int, error) {
	if !r.started {
		r.start()
	}
	for r.pos >= len(r.out) {
		if r.err != nil {
			return 0, r.err
		}
		if r.final {
			return 0, io.EOF
		}
		r.err = r.next()
	}
	n := copy(p, r.out[r.pos:])
	r.pos += n
	return n, nil
}

func (r *streamReader) next() error {
	n, err := io.ReadFull(r.reader, r.in)
	short := errors.Is(err, io.ErrUnexpectedEOF)
	switch {
	case err == nil || short:
	case errors.Is(err, io.EOF):
		return errStreamTruncated
	default:
		return err
	}
	out, tag, err := r.state.pull(r.out[:0], r.in[:n])
	if err != nil {
		return err
	}
	switch {
	case tag == streamTagFinal && !short:
		return errors.New("premature end")
	case tag == streamTagFinal:
		r.final = true
	case short:
		return errStreamTruncated
	}
	r.out, r.pos = out, 0
	return nil
}
