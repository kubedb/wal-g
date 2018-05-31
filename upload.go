package walg

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

// MAXRETRIES is the maximum number of retries for upload.
var MAXRETRIES = 7

// Configure connects to S3 and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
//
// Requires these environment variables to be set:
// WALE_S3_PREFIX
//
// Able to configure the upload part size in the S3 uploader.
/*
func Configure() (*S3TarUploader, *Prefix, error) {
	waleS3Prefix := os.Getenv("WALE_S3_PREFIX")
	if waleS3Prefix == "" {
		return nil, nil, &UnsetEnvVarError{Names: []string{"WALE_S3_PREFIX"}}
	}

	u, err := url.Parse(waleS3Prefix)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Configure: failed to parse url '%s'", waleS3Prefix)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, nil, fmt.Errorf("Missing url scheme=%q and/or host=%q", u.Scheme, u.Host)
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

	config.MaxRetries = &MAXRETRIES
	if _, err := config.Credentials.Get(); err != nil {
		return nil, nil, errors.Wrapf(err, "Configure: failed to get AWS credentials; please specify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}

	if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
		config.Endpoint = aws.String(endpoint)
	}

	s3ForcePathStyleStr := os.Getenv("AWS_S3_FORCE_PATH_STYLE")
	if len(s3ForcePathStyleStr) > 0 {
		s3ForcePathStyle, err := strconv.ParseBool(s3ForcePathStyleStr)
		if err != nil {
			return nil, nil, errors.Wrap(err, "Configure: failed parse AWS_S3_FORCE_PATH_STYLE")
		}
		config.S3ForcePathStyle = aws.Bool(s3ForcePathStyle)
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region, err = findS3BucketRegion(bucket, config)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "Configure: AWS_REGION is not set and s3:GetBucketLocation failed")
		}
	}
	config = config.WithRegion(region)

	pre := &Prefix{
		Bucket: aws.String(bucket),
		Server: aws.String(server),
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Configure: failed to create new session")
	}

	pre.Svc = s3.New(sess)

	upload := NewS3TarUploader(pre.Svc, bucket, server, region)

	var con = GetMaxUploadConcurrency(10)
	storageClass, ok := os.LookupEnv("WALG_S3_STORAGE_CLASS")
	if ok {
		upload.StorageClass = storageClass
	}

	upload.Upl = CreateUploader(pre.Svc, 20*1024*1024, con) //default 10 concurrency streams at 20MB

	return upload, pre, err
}
*/

// HandleSentinel uploads the compressed tar file of `pg_control`. Will only be called
// after the rest of the backup is successfully uploaded to S3. Returns
// an error upon failure.
func (bundle *Bundle) HandleSentinel() error {
	fileName := bundle.Sen.Info.Name()
	info := bundle.Sen.Info
	path := bundle.Sen.path

	bundle.NewTarBall(false)
	tarBall := bundle.Tb
	tarBall.SetUp(&bundle.Crypter, "pg_control.tar.lz4")
	tarWriter := tarBall.Tw()

	hdr, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to grab header info")
	}

	hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
	fmt.Println(hdr.Name)

	err = tarWriter.WriteHeader(hdr)
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to write header")
	}

	if info.Mode().IsRegular() {
		f, err := os.Open(path)
		if err != nil {
			return errors.Wrapf(err, "HandleSentinel: failed to open file %s\n", path)
		}

		lim := &io.LimitedReader{
			R: f,
			N: int64(hdr.Size),
		}

		_, err = io.Copy(tarWriter, lim)
		if err != nil {
			return errors.Wrap(err, "HandleSentinel: copy failed")
		}

		tarBall.AddSize(hdr.Size)
		f.Close()
	}

	err = tarBall.CloseTar()
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to close tarball")
	}

	return nil
}

// HandleLabelFiles creates the `backup_label` and `tablespace_map` Files and uploads
// it to S3 by stopping the backup. Returns error upon failure.
func (bundle *Bundle) HandleLabelFiles(conn *pgx.Conn) (uint64, error) {
	var lb string
	var sc string
	var lsnStr string

	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: Failed to build query runner.")
	}
	lb, sc, lsnStr, err = queryRunner.StopBackup()
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to stop backup")
	}

	lsn, err := ParseLsn(lsnStr)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to parse finish LSN")
	}

	if queryRunner.Version < 90600 {
		return lsn, nil
	}

	bundle.NewTarBall(false)
	tarBall := bundle.Tb
	tarBall.SetUp(&bundle.Crypter)
	tarWriter := tarBall.Tw()

	lhdr := &tar.Header{
		Name:     "backup_label",
		Mode:     int64(0600),
		Size:     int64(len(lb)),
		Typeflag: tar.TypeReg,
	}

	err = tarWriter.WriteHeader(lhdr)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to write header")
	}
	_, err = io.Copy(tarWriter, strings.NewReader(lb))
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: copy failed")
	}
	fmt.Println(lhdr.Name)

	shdr := &tar.Header{
		Name:     "tablespace_map",
		Mode:     int64(0600),
		Size:     int64(len(sc)),
		Typeflag: tar.TypeReg,
	}

	err = tarWriter.WriteHeader(shdr)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to write header")
	}
	_, err = io.Copy(tarWriter, strings.NewReader(sc))
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: copy failed")
	}
	fmt.Println(shdr.Name)

	err = tarBall.CloseTar()
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to close tarball")
	}

	return lsn, nil
}
