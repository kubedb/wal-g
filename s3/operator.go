package s3

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g"
)

type S3Operator struct {
	Prefix *Prefix
	Region *string
	Tu     *S3TarUploader
}

func init() {
	walg.RegisterOperator("s3", &S3Operator{})
	walg.RegisterOperator("aws", &S3Operator{})
}

func (s *S3Operator) GetServer() *string {
	return s.Prefix.Server
}

func (s *S3Operator) GetBucket() *string {
	return s.Prefix.Bucket
}

func (s *S3Operator) CheckExistence(archive *string) (bool, error) {
	arch := &s3.HeadObjectInput{
		Bucket: s.Prefix.Bucket,
		Key:    archive,
	}

	_, err := s.Prefix.Svc.HeadObject(arch)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case "NotFound":
				return false, nil
			default:
				return false, awsErr
			}
		}
	}
	return true, nil
}

func (s *S3Operator) GetETag(archive *string) (*string, error) {
	arch := &s3.HeadObjectInput{
		Bucket: s.Prefix.Bucket,
		Key:    archive,
	}

	h, err := s.Prefix.Svc.HeadObject(arch)
	if err != nil {
		return nil, err
	}

	return h.ETag, nil
}

func (s *S3Operator) GetArchive(archive *string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: s.Prefix.Bucket,
		Key:    archive,
	}

	a, err := s.Prefix.Svc.GetObject(input)
	if err != nil {
		return nil, errors.Wrap(err, "GetArchive: s3.GetObject failed")
	}

	return a.Body, nil
}

func (s *S3Operator) GetTarUploader() (walg.TarUploaderInterface, error) {
	if s.Tu != nil {
		return s.Tu, nil
	}

	if s.Prefix == nil {
		if _, err := s.GetCloud(); err != nil {
			return nil, err
		}
	}

	pre := s.Prefix
	upload := NewS3TarUploader(pre.Svc, aws.StringValue(pre.Bucket), aws.StringValue(pre.Server), aws.StringValue(s.Region))

	var con = walg.GetMaxUploadConcurrency(10)
	storageClass, ok := os.LookupEnv("WALG_S3_STORAGE_CLASS")
	if ok {
		upload.StorageClass = storageClass
	}

	upload.Upl = CreateUploader(pre.Svc, 20*1024*1024, con) //default 10 concurrency streams at 20MB

	s.Tu = upload

	return s.Tu, nil
}

var _ walg.Operator = &S3Operator{}

var _ walg.Cloud = &S3Operator{}

func (s *S3Operator) GetCloud() (walg.Cloud, error) {
	if s.Prefix != nil {
		return s, nil
	}

	waleS3Prefix := os.Getenv("WALE_S3_PREFIX")
	if waleS3Prefix == "" {
		return nil, &walg.UnsetEnvVarError{Names: []string{"WALE_S3_PREFIX"}}
	}

	u, err := url.Parse(waleS3Prefix)
	if err != nil {
		return nil, errors.Wrapf(err, "Configure: failed to parse url '%s'", waleS3Prefix)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("Missing url scheme=%q and/or host=%q", u.Scheme, u.Host)
	}

	bucket := u.Host
	var server = ""
	if len(u.Path) > 0 {
		// TODO: Unchecked assertion: first char is '/'
		server = u.Path[1:]
	}

	if len(server) > 0 && server[len(server)-1] == '/' {
		// Allover the code this parameter is concatenated with '/'.
		// TODO: Get rid of numerous string literals concatenated with this
		server = server[:len(server)-1]
	}

	config := defaults.Get().Config

	config.MaxRetries = &walg.MAXRETRIES
	if _, err := config.Credentials.Get(); err != nil {
		return nil, errors.Wrapf(err, "Configure: failed to get AWS credentials; please specify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}

	if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
		config.Endpoint = aws.String(endpoint)
	}

	s3ForcePathStyleStr := os.Getenv("AWS_S3_FORCE_PATH_STYLE")
	if len(s3ForcePathStyleStr) > 0 {
		s3ForcePathStyle, err := strconv.ParseBool(s3ForcePathStyleStr)
		if err != nil {
			return nil, errors.Wrap(err, "Configure: failed parse AWS_S3_FORCE_PATH_STYLE")
		}
		config.S3ForcePathStyle = aws.Bool(s3ForcePathStyle)
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region, err = findS3BucketRegion(bucket, config)
		if err != nil {
			return nil, errors.Wrapf(err, "Configure: AWS_REGION is not set and s3:GetBucketLocation failed")
		}
	}

	s.Region = aws.String(region)

	config = config.WithRegion(region)

	pre := &Prefix{
		Bucket: aws.String(bucket),
		Server: aws.String(server),
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, errors.Wrap(err, "Configure: failed to create new session")
	}

	pre.Svc = s3.New(sess)

	s.Prefix = pre

	return s, nil
}

func (s *S3Operator) BackupPush(dirArc string, bk *walg.Backup) {

	maxDeltas, fromFull := walg.GetDeltaConfig()

	var dto walg.TarBallSentinelDto
	var latest string
	var err error
	incrementCount := 1

	if maxDeltas > 0 {
		latest, err = bk.GetLatest()
		if err != walg.ErrLatestNotFound {
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			dto = walg.FetchSentinel(latest, bk, bk.Prefix)
			if dto.IncrementCount != nil {
				incrementCount = *dto.IncrementCount + 1
			}

			if incrementCount > maxDeltas {
				fmt.Println("Reached max delta steps. Doing full backup.")
				dto = walg.TarBallSentinelDto{}
			} else if dto.LSN == nil {
				fmt.Println("LATEST backup was made without support for delta feature. Fallback to full backup with LSN marker for future deltas.")
			} else {
				if fromFull {
					fmt.Println("Delta will be made from full backup.")
					latest = *dto.IncrementFullName
					dto = walg.FetchSentinel(latest, bk, bk.Prefix)
				}
				fmt.Printf("Delta backup from %v with LSN %x. \n", latest, *dto.LSN)
			}
		}
	}

	bundle := &walg.Bundle{
		MinSize:            int64(1000000000), //MINSIZE = 1GB
		IncrementFromLsn:   dto.LSN,
		IncrementFromFiles: dto.Files,
		Files:              &sync.Map{},
	}
	if dto.Files == nil {
		bundle.IncrementFromFiles = make(map[string]walg.BackupFileDescription)
	}

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := walg.Connect()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	name, lsn, pgVersion, err := bundle.StartBackup(conn, time.Now().String())
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	if len(latest) > 0 && dto.LSN != nil {
		name = name + "_D_" + walg.StripWalFileName(latest)
	}

	// Start a new tar bundle and walk the DIRARC directory and upload to S3.
	bundle.Tbm = &S3TarBallMaker{
		BaseDir:          filepath.Base(dirArc),
		Trim:             dirArc,
		BkupName:         name,
		Tu:               s.Tu,
		Lsn:              &lsn,
		IncrementFromLsn: dto.LSN,
		IncrementFrom:    latest,
	}

	bundle.StartQueue()
	fmt.Println("Walking ...")
	err = filepath.Walk(dirArc, bundle.TarWalker)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	err = bundle.FinishQueue()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Upload `pg_control`.
	err = bundle.HandleSentinel()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	finishLsn, err := bundle.HandleLabelFiles(conn)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	timelineChanged := bundle.CheckTimelineChanged(conn)
	var sentinel *walg.TarBallSentinelDto

	if !timelineChanged {
		sentinel = &walg.TarBallSentinelDto{
			LSN:              &lsn,
			IncrementFromLSN: dto.LSN,
			PgVersion:        pgVersion,
		}
		if dto.LSN != nil {
			sentinel.IncrementFrom = &latest
			sentinel.IncrementFullName = &latest
			if dto.IsIncremental() {
				sentinel.IncrementFullName = dto.IncrementFullName
			}
			sentinel.IncrementCount = &incrementCount
		}

		sentinel.SetFiles(bundle.GetFiles())
		sentinel.FinishLSN = &finishLsn
	}

	// Wait for all uploads to finish.
	err = bundle.Tb.Finish(sentinel)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
}

// GetBackups receives backup descriptions and sorts them by time
func (s *S3Operator) GetBackups(bk *walg.Backup) ([]walg.BackupTime, error) {
	bucket := bk.Prefix.GetBucket()
	var sortTimes []walg.BackupTime
	objects := &s3.ListObjectsV2Input{
		Bucket:    bucket,
		Prefix:    bk.Path,
		Delimiter: aws.String("/"),
	}

	var backups = make([]*s3.Object, 0)

	err := s.Prefix.Svc.ListObjectsV2Pages(objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		backups = append(backups, files.Contents...)
		return true
	})

	if err != nil {
		return nil, errors.Wrap(err, "GetLatest: s3.ListObjectsV2 failed")
	}

	count := len(backups)

	if count == 0 {
		return nil, walg.ErrLatestNotFound
	}

	sortTimes = getBackupTimeSlices(backups)

	return sortTimes, nil
}

func (s *S3Operator) GetReaderMaker(key string) walg.ReaderMaker {
	srm := &S3ReaderMaker{
		Prefix:     s.Prefix,
		Key:        aws.String(key),
		FileFormat: walg.CheckType(key),
	}

	return srm
}

func (s *S3Operator) DeleteWALBefore(bt walg.BackupTime, bk *walg.Backup) {
	objects, err := s.GetWals(bt.WalFileName)
	if err != nil {
		log.Fatal("Unable to obtaind WALS for border ", bt.Name, err)
	}
	parts := partitionObjects(objects, 1000)
	for _, part := range parts {
		input := &s3.DeleteObjectsInput{Bucket: s.Prefix.Bucket, Delete: &s3.Delete{
			Objects: part,
		}}
		_, err = s.Prefix.Svc.DeleteObjects(input)
		if err != nil {
			log.Fatal("Unable to delete WALS before ", bt.Name, err)
		}
	}
}

func (s *S3Operator) GetWals(before string) ([]*s3.ObjectIdentifier, error) {
	objects := &s3.ListObjectsV2Input{
		Bucket: s.Prefix.Bucket,
		Prefix: aws.String(walg.SanitizePath(*s.Prefix.Server)),
	}

	arr := make([]*s3.ObjectIdentifier, 0)

	err := s.Prefix.Svc.ListObjectsV2Pages(objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, ob := range files.Contents {
			key := *ob.Key
			if walg.StripWalName(key) < before {
				arr = append(arr, &s3.ObjectIdentifier{Key: aws.String(key)})
			}
		}
		return true
	})

	if err != nil {
		return nil, errors.Wrap(err, "GetKeys: s3.ListObjectsV2 failed")
	}

	return arr, nil
}

func (s *S3Operator) DropBackup(name *string) {
	tarFiles, err := s.GetKeys(name)
	if err != nil {
		log.Fatal("Unable to list backup for deletion ", *name, err)
	}

	folderKey := strings.TrimPrefix(*s.Prefix.Server+"/basebackups_005/"+*name, "/")
	suffixKey := folderKey + walg.SentinelSuffix

	keys := append(tarFiles, suffixKey, folderKey)
	parts := walg.Partition(keys, 1000)
	for _, part := range parts {

		input := &s3.DeleteObjectsInput{Bucket: s.Prefix.Bucket, Delete: &s3.Delete{
			Objects: partitionToObjects(part),
		}}
		_, err = s.Prefix.Svc.DeleteObjects(input)
		if err != nil {
			log.Fatal("Unable to delete backup ", name, err)
		}

	}
}

// GetKeys returns all the keys for the Files in the specified backup.
func (s *S3Operator) GetKeys(name *string) ([]string, error) {
	objects := &s3.ListObjectsV2Input{
		Bucket: s.Prefix.Bucket,
		Prefix: aws.String(walg.SanitizePath(*s.Prefix.Server + *name + "/tar_partitions")),
	}

	result := make([]string, 0)

	err := s.Prefix.Svc.ListObjectsV2Pages(objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {

		arr := make([]string, len(files.Contents))

		for i, ob := range files.Contents {
			key := *ob.Key
			arr[i] = key
		}

		result = append(result, arr...)
		return true
	})
	if err != nil {
		return nil, errors.Wrap(err, "GetKeys: s3.ListObjectsV2 failed")
	}

	return result, nil
}
