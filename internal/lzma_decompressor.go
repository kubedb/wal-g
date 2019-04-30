package internal

import (
	"github.com/pkg/errors"
	"github.com/ulikunitz/xz/lzma"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type LzmaDecompressor struct{}

func (decompressor LzmaDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	lzReader, err := lzma.NewReader(NewUntilEofReader(src))
	if err != nil {
		return errors.Wrap(err, "DecompressLzma: lzma reader creation failed")
	}
	_, err = utility.FastCopy(dst, lzReader)
	return errors.Wrap(err, "DecompressLzma: lzma write failed")
}

func (decompressor LzmaDecompressor) FileExtension() string {
	return LzmaFileExtension
}
