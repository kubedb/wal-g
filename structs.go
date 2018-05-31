package walg

import (
	"archive/tar"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
)


type TarUploaderInterface interface {
	Finish()
	//Clone() TarUploaderInterface
	UploadWal(string, Cloud, bool) (string, error)
}

// EXCLUDE is a list of excluded members from the bundled backup.
var EXCLUDE = make(map[string]Empty)

func init() {
	EXCLUDE["pg_log"] = Empty{}
	EXCLUDE["pg_xlog"] = Empty{}
	EXCLUDE["pg_wal"] = Empty{}

	EXCLUDE["pgsql_tmp"] = Empty{}
	EXCLUDE["postgresql.auto.conf.tmp"] = Empty{}
	EXCLUDE["postmaster.pid"] = Empty{}
	EXCLUDE["postmaster.opts"] = Empty{}
	EXCLUDE["recovery.conf"] = Empty{}

	// DIRECTORIES
	EXCLUDE["pg_dynshmem"] = Empty{}
	EXCLUDE["pg_notify"] = Empty{}
	EXCLUDE["pg_replslot"] = Empty{}
	EXCLUDE["pg_serial"] = Empty{}
	EXCLUDE["pg_stat_tmp"] = Empty{}
	EXCLUDE["pg_snapshots"] = Empty{}
	EXCLUDE["pg_subtrans"] = Empty{}
}

// Empty is used for channel signaling.
type Empty struct{}

// NilWriter to /dev/null
type NilWriter struct{}

// Write to /dev/null
func (nw *NilWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

// TarBundle represents one completed directory.
type TarBundle interface {
	NewTarBall(dedicatedUploader bool)
	GetIncrementBaseLsn() *uint64
	GetIncrementBaseFiles() BackupFileList

	StartQueue()
	Deque() TarBall
	EnqueueBack(tb TarBall, parallelOpInProgress *bool)
	CheckSizeAndEnqueueBack(tb TarBall) error
	FinishQueue() error
	GetFiles() *sync.Map
}

// A Bundle represents the directory to
// be walked. Contains at least one TarBall
// if walk has started. Each TarBall will be at least
// MinSize bytes. The Sentinel is used to ensure complete
// uploaded backups; in this case, pg_control is used as
// the sentinel.
type Bundle struct {
	MinSize            int64
	Sen                *Sentinel
	Tb                 TarBall
	Tbm                TarBallMaker
	Crypter            OpenPGPCrypter
	Timeline           uint32
	Replica            bool
	IncrementFromLsn   *uint64
	IncrementFromFiles BackupFileList

	tarballQueue     chan (TarBall)
	uploadQueue      chan (TarBall)
	parallelTarballs int
	maxUploadQueue   int
	mutex            sync.Mutex
	started          bool

	Files *sync.Map
}

func (b *Bundle) GetFiles() *sync.Map { return b.Files }

func (b *Bundle) StartQueue() {
	if b.started {
		panic("Trying to start already started Queue")
	}
	b.parallelTarballs = getMaxUploadDiskConcurrency()
	b.maxUploadQueue = getMaxUploadQueue()
	b.tarballQueue = make(chan (TarBall), b.parallelTarballs)
	b.uploadQueue = make(chan (TarBall), b.parallelTarballs+b.maxUploadQueue)
	for i := 0; i < b.parallelTarballs; i++ {
		b.NewTarBall(true)
		b.tarballQueue <- b.Tb
	}
	b.started = true
}

func (b *Bundle) Deque() TarBall {
	if !b.started {
		panic("Trying to deque from not started Queue")
	}
	return <-b.tarballQueue
}

func (b *Bundle) FinishQueue() error {
	if !b.started {
		panic("Trying to stop not started Queue")
	}
	b.started = false

	// At this point no new tarballs should be put into uploadQueue
	for len(b.uploadQueue) > 0 {
		select {
		case otb := <-b.uploadQueue:
			otb.AwaitUploads()
		default:
		}
	}

	// We have to deque exactly this count of workers
	for i := 0; i < b.parallelTarballs; i++ {
		tb := <-b.tarballQueue
		if tb.Tw() == nil {
			// This had written nothing
			continue
		}
		err := tb.CloseTar()
		if err != nil {
			return errors.Wrap(err, "TarWalker: failed to close tarball")
		}
		tb.AwaitUploads()
	}
	return nil
}

func (b *Bundle) EnqueueBack(tb TarBall, parallelOpInProgress *bool) {
	if !*parallelOpInProgress {
		b.tarballQueue <- tb
	}
}

func (b *Bundle) CheckSizeAndEnqueueBack(tb TarBall) error {
	if tb.Size() > b.MinSize {
		b.mutex.Lock()
		defer b.mutex.Unlock()

		err := tb.CloseTar()
		if err != nil {
			return errors.Wrap(err, "TarWalker: failed to close tarball")
		}

		b.uploadQueue <- tb
		for len(b.uploadQueue) > b.maxUploadQueue {
			select {
			case otb := <-b.uploadQueue:
				otb.AwaitUploads()
			default:
			}
		}

		b.NewTarBall(true)
		tb = b.Tb
	}
	b.tarballQueue <- tb
	return nil
}

// NewTarBall starts writing new tarball
func (b *Bundle) NewTarBall(dedicatedUploader bool) {
	ntb := b.Tbm.Make(dedicatedUploader)

	b.Tb = ntb
}

// GetIncrementBaseLsn returns LSN of previous backup
func (b *Bundle) GetIncrementBaseLsn() *uint64 { return b.IncrementFromLsn }

// GetIncrementBaseFiles returns list of Files from previous backup
func (b *Bundle) GetIncrementBaseFiles() BackupFileList { return b.IncrementFromFiles }

// Sentinel is used to signal completion of a walked
// directory.
type Sentinel struct {
	Info os.FileInfo
	path string
}

// A TarBall represents one tar file.
type TarBall interface {
	SetUp(crypter Crypter, args ...string)
	CloseTar() error
	Finish(sentinel *TarBallSentinelDto) error
	BaseDir() string
	Trim() string
	Nop() bool
	Number() int
	Size() int64
	AddSize(int64)
	Tw() *tar.Writer
	AwaitUploads()
}

// BackupFileList is a map of file properties in a backup
type BackupFileList map[string]BackupFileDescription


// ErrSentinelNotUploaded happens when upload of json sentinel failed
var ErrSentinelNotUploaded = errors.New("Sentinel was not uploaded due to timeline change during backup")

// TarBallSentinelDto describes file structure of json sentinel
type TarBallSentinelDto struct {
	LSN               *uint64
	IncrementFromLSN  *uint64 `json:"DeltaFromLSN,omitempty"`
	IncrementFrom     *string `json:"DeltaFrom,omitempty"`
	IncrementFullName *string `json:"DeltaFullName,omitempty"`
	IncrementCount    *int    `json:"DeltaCount,omitempty"`

	Files BackupFileList

	PgVersion int
	FinishLSN *uint64

	UserData interface{} `json:"UserData,omitempty"`
}

func (s *TarBallSentinelDto) SetFiles(p *sync.Map) {
	s.Files = make(BackupFileList)
	p.Range(func(k, v interface{}) bool {
		key := k.(string)
		description := v.(BackupFileDescription)
		s.Files[key] = description
		return true
	})
}

// BackupFileDescription contains properties of one backup file
type BackupFileDescription struct {
	IsIncremented bool // should never be both incremented and Skipped
	IsSkipped     bool
	MTime         time.Time
}

// IsIncremental checks that sentinel represents delta backup
func (dto *TarBallSentinelDto) IsIncremental() bool {
	// If we have increment base, we must have all the rest properties.
	// If we do not have base - anything else is a mistake
	if dto.IncrementFrom != nil {
		if dto.IncrementFromLSN == nil || dto.IncrementFullName == nil || dto.IncrementCount == nil {
			panic("Inconsistent TarBallSentinelDto")
		}
	} else if dto.IncrementFromLSN != nil && dto.IncrementFullName != nil && dto.IncrementCount != nil {
		panic("Inconsistent TarBallSentinelDto")
	}
	return dto.IncrementFrom != nil
}
