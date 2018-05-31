package s3


import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g"
)

// Helper function to upload to S3. If an error occurs during upload, retries will
// occur in exponentially incremental seconds.
func (tu *S3TarUploader) upload(input *s3manager.UploadInput, path string) (err error) {
	upl := tu.Upl

	_, e := upl.Upload(input)
	if e == nil {
		tu.Success = true
		return nil
	}

	if multierr, ok := e.(s3manager.MultiUploadFailure); ok {
		log.Printf("upload: failed to upload '%s' with UploadID '%s'.", path, multierr.UploadID())
	} else {
		log.Printf("upload: failed to upload '%s': %s.", path, e.Error())
	}
	return e
}

// createUploadInput creates a s3manager.UploadInput for a S3TarUploader using
// the specified path and reader.
func (tu *S3TarUploader) createUploadInput(path string, reader io.Reader) *s3manager.UploadInput {
	return &s3manager.UploadInput{
		Bucket:       aws.String(tu.bucket),
		Key:          aws.String(path),
		Body:         reader,
		StorageClass: aws.String(tu.StorageClass),
	}
}

// UploadWal compresses a WAL file using LZ4 and uploads to S3. Returns
// the first error encountered and an empty string upon failure.
func (tu S3TarUploader) UploadWal(path string, cloud walg.Cloud, verify bool) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", errors.Wrapf(err, "UploadWal: failed to open file %s\n", path)
	}

	lz := &walg.LzPipeWriter{
		Input: f,
	}

	lz.Compress(&walg.OpenPGPCrypter{})

	p := walg.SanitizePath(tu.server + "/wal_005/" + filepath.Base(path) + ".lz4")
	reader := lz.Output

	if verify {
		reader = walg.NewMd5Reader(reader)
	}

	input := tu.createUploadInput(p, reader)

	tu.wg.Add(1)
	go func() {
		defer tu.wg.Done()
		err = tu.upload(input, path)

	}()

	tu.Finish()
	fmt.Println("WAL PATH:", p)
	if verify {
		sum := reader.(*walg.Md5Reader).Sum()
		a := &walg.Archive{
			Prefix:  cloud,
			Archive: aws.String(p),
		}
		eTag, err := a.GetETag()
		if err != nil {
			log.Fatalf("Unable to verify WAL %s", err)
		}
		if eTag == nil {
			log.Fatalf("Unable to verify WAL: nil ETag ")
		}

		trimETag := strings.Trim(*eTag, "\"")
		if sum != trimETag {
			log.Fatalf("WAL verification failed: md5 %s ETag %s", sum, trimETag)
		}
		fmt.Println("ETag ", trimETag)
	}
	return p, err
}

// Given an S3 bucket name, attempt to determine its Region
func findS3BucketRegion(bucket string, config *aws.Config) (string, error) {
	input := s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	}

	sess, err := session.NewSession(config.WithRegion("us-east-1"))
	if err != nil {
		return "", err
	}

	output, err := s3.New(sess).GetBucketLocation(&input)
	if err != nil {
		return "", err
	}

	if output.LocationConstraint == nil {
		// buckets in "US Standard", a.k.a. us-east-1, are returned as a nil Region
		return "us-east-1", nil
	}
	// all other regions are strings
	return *output.LocationConstraint, nil
}

// CreateUploader returns an uploader with customizable concurrency
// and partsize.
func CreateUploader(svc s3iface.S3API, partsize, concurrency int) s3manageriface.UploaderAPI {
	up := s3manager.NewUploaderWithClient(svc, func(u *s3manager.Uploader) {
		u.PartSize = int64(partsize)
		u.Concurrency = concurrency
	})
	return up
}

// StartUpload creates a lz4 writer and runs upload in the background once
// a compressed tar member is finished writing.
func (s *S3TarBall) StartUpload(name string, crypter walg.Crypter) io.WriteCloser {
	pr, pw := io.Pipe()
	tupl := s.tu

	path := tupl.server + "/basebackups_005/" + s.bkupName + "/tar_partitions/" + name
	input := tupl.createUploadInput(path, pr)

	fmt.Printf("Starting part %d ...\n", s.number)

	tupl.wg.Add(1)
	go func() {
		defer tupl.wg.Done()

		err := tupl.upload(input, path)
		if re, ok := err.(walg.Lz4Error); ok {

			log.Printf("FATAL: could not upload '%s' due to compression error\n%+v\n", path, re)
		}
		if err != nil {
			log.Printf("upload: could not upload '%s'\n", path)
			log.Printf("FATAL%v\n", err)
		}

	}()

	if crypter.IsUsed() {
		wc, err := crypter.Encrypt(pw)

		if err != nil {
			log.Fatal("upload: encryption error ", err)
		}

		return &walg.Lz4CascadeClose2{lz4.NewWriter(wc), wc, pw}
	}

	return &walg.Lz4CascadeClose{lz4.NewWriter(pw), pw}
}
