package s3

import "github.com/wal-g/wal-g"

// S3TarBallMaker creates tarballs that are uploaded to S3.
type S3TarBallMaker struct {
	number           int
	BaseDir          string
	Trim             string
	BkupName         string
	Tu               *S3TarUploader
	Lsn              *uint64
	IncrementFromLsn *uint64
	IncrementFrom    string
}

// Make returns a tarball with required S3 fields.
func (s *S3TarBallMaker) Make(dedicatedUploader bool) walg.TarBall {
	s.number++
	uploader := s.Tu
	if dedicatedUploader {
		uploader = uploader.Clone()
	}
	return &S3TarBall{
		number:           s.number,
		baseDir:          s.BaseDir,
		trim:             s.Trim,
		bkupName:         s.BkupName,
		tu:               uploader,
		Lsn:              s.Lsn,
		IncrementFromLsn: s.IncrementFromLsn,
		IncrementFrom:    s.IncrementFrom,
	}
}
