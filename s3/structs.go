package s3


import (
	"sync"

	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g"
)

// S3TarUploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader. Must call CreateUploader()
// in 'upload.go'.
type S3TarUploader struct {
	Upl          s3manageriface.UploaderAPI
	StorageClass string
	Success      bool
	bucket       string
	server       string
	region       string
	wg           *sync.WaitGroup
}

var _ walg.TarUploaderInterface = &S3TarUploader{}

// Finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (tu *S3TarUploader) Finish() {
	tu.wg.Wait()
	if !tu.Success {
		log.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar S3TarUploader with new WaitGroup
func (tu *S3TarUploader) Clone() *S3TarUploader {
	return &S3TarUploader{
		tu.Upl,
		tu.StorageClass,
		tu.Success,
		tu.bucket,
		tu.server,
		tu.region,
		&sync.WaitGroup{},
	}
}

// NewS3TarUploader creates a new tar uploader without the actual
// S3 uploader. CreateUploader() is used to configure byte size and
// concurrency streams for the uploader.
func NewS3TarUploader(svc s3iface.S3API, bucket, server, region string) *S3TarUploader {
	return &S3TarUploader{
		StorageClass: "STANDARD",
		bucket:       bucket,
		server:       server,
		region:       region,
		wg:           &sync.WaitGroup{},
	}
}

// Finish writes an empty .json file and uploads it with the
// the backup name. Finish will wait until all tar file parts
// have been uploaded. The json file will only be uploaded
// if all other parts of the backup are present in S3.
// an alert is given with the corresponding error.
func (s *S3TarBall) Finish(sentinel *walg.TarBallSentinelDto) error {
	var err error
	name := s.bkupName + "_backup_stop_sentinel.json"
	tupl := s.tu

	tupl.Finish()

	//If other parts are successful in uploading, upload json file.
	if tupl.Success && sentinel != nil {
		sentinel.UserData = walg.GetSentinelUserData()
		dtoBody, err := json.Marshal(*sentinel)
		if err != nil {
			return err
		}
		path := tupl.server + "/basebackups_005/" + name
		input := &s3manager.UploadInput{
			Bucket:       aws.String(tupl.bucket),
			Key:          aws.String(path),
			Body:         bytes.NewReader(dtoBody),
			StorageClass: aws.String(tupl.StorageClass),
		}

		tupl.wg.Add(1)
		go func() {
			defer tupl.wg.Done()

			e := tupl.upload(input, path)
			if e != nil {
				log.Printf("upload: could not upload '%s'\n", path)
				log.Fatalf("S3TarBall Finish: json failed to upload")
			}
		}()

		tupl.Finish()
	} else {
		log.Printf("Uploaded %d compressed tar Files.\n", s.number)
		log.Printf("Sentinel was not uploaded %v", name)
		return walg.ErrSentinelNotUploaded
	}

	if err == nil && tupl.Success {
		fmt.Printf("Uploaded %d compressed tar Files.\n", s.number)
	}
	return err
}

// BaseDir of a backup
func (s *S3TarBall) BaseDir() string { return s.baseDir }

// Trim suffix
func (s *S3TarBall) Trim() string { return s.trim }

// Nop is a dummy fonction for test purposes
func (s *S3TarBall) Nop() bool { return s.nop }

// Number of parts
func (s *S3TarBall) Number() int { return s.number }

// Size accumulated in this tarball
func (s *S3TarBall) Size() int64 { return s.size }

// AddSize to total Size
func (s *S3TarBall) AddSize(i int64) { s.size += i }

// Tw is tar writer
func (s *S3TarBall) Tw() *tar.Writer { return s.tw }

// S3TarBall represents a tar file that is
// going to be uploaded to S3.
type S3TarBall struct {
	baseDir          string
	trim             string
	bkupName         string
	nop              bool
	number           int
	size             int64
	w                io.WriteCloser
	tw               *tar.Writer
	tu               *S3TarUploader
	Lsn              *uint64
	IncrementFromLsn *uint64
	IncrementFrom    string
	Files            walg.BackupFileList
}

// SetUp creates a new tar writer and starts upload to S3.
// Upload will block until the tar file is finished writing.
// If a name for the file is not given, default name is of
// the form `part_....tar.lz4`.
func (s *S3TarBall) SetUp(crypter walg.Crypter, names ...string) {
	if s.tw == nil {
		var name string
		if len(names) > 0 {
			name = names[0]
		} else {
			name = "part_" + fmt.Sprintf("%0.3d", s.number) + ".tar.lz4"
		}
		w := s.StartUpload(name, crypter)

		s.w = w
		s.tw = tar.NewWriter(w)
	}
}

// CloseTar closes the tar writer, flushing any unwritten data
// to the underlying writer before also closing the underlying writer.
func (s *S3TarBall) CloseTar() error {
	err := s.tw.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close tar writer")
	}

	err = s.w.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close underlying writer")
	}
	fmt.Printf("Finished writing part %d.\n", s.number)
	return nil
}

func (b *S3TarBall) AwaitUploads() {
	b.tu.wg.Wait()
}
