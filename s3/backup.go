package s3

import (
	"io"
	"sort"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g"
)

// S3ReaderMaker handles cases where backups need to be uploaded to
// S3.
type S3ReaderMaker struct {
	Prefix     *Prefix
	Key        *string
	FileFormat string
}

// Format of a file
func (s *S3ReaderMaker) Format() string { return s.FileFormat }

// Path to file in bucket
func (s *S3ReaderMaker) Path() string { return *s.Key }

// Reader creates a new S3 reader for each S3 object.
func (s *S3ReaderMaker) Reader() (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: s.Prefix.Bucket,
		Key:    s.Key,
	}

	rdr, err := s.Prefix.Svc.GetObject(input)
	if err != nil {
		return nil, errors.Wrap(err, "S3 Reader: s3.GetObject failed")
	}
	return rdr.Body, nil

}

// Prefix contains the S3 service client, bucket and string.
type Prefix struct {
	Svc    s3iface.S3API
	Bucket *string
	Server *string
}

// getBackupTimeSlices converts S3 objects to backup description
func getBackupTimeSlices(backups []*s3.Object) []walg.BackupTime {
	sortTimes := make([]walg.BackupTime, len(backups))
	for i, ob := range backups {
		key := *ob.Key
		time := *ob.LastModified
		sortTimes[i] = walg.BackupTime{walg.StripNameBackup(key), time, walg.StripWalFileName(key)}
	}
	slice := walg.TimeSlice(sortTimes)
	sort.Sort(slice)
	return slice
}
