package walg

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime/pprof"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pkg/errors"
)

// HandleDelete is invoked to perform wal-g delete
func HandleDelete(cloud Cloud, args []string) {
	cfg := ParseDeleteArguments(args, printDeleteUsageAndFail)

	server := cloud.GetServer()
	var bk = &Backup{
		Prefix: cloud,
		Path:   GetBackupPath(server),
	}

	if cfg.before {
		if cfg.beforeTime == nil {
			deleteBeforeTarget(cfg.target, bk, cloud, cfg.findFull, nil, cfg.dryrun)
		} else {
			backups, err := bk.GetBackups()
			if err != nil {
				log.Fatal(err)
			}
			for _, b := range backups {
				if b.Time.Before(*cfg.beforeTime) {
					deleteBeforeTarget(b.Name, bk, cloud, cfg.findFull, backups, cfg.dryrun)
					return
				}
			}
			log.Println("No backups before ", *cfg.beforeTime)
		}
	}
	if cfg.retain {
		number, err := strconv.Atoi(cfg.target)
		if err != nil {
			log.Fatal("Unable to parse number of backups: ", err)
		}
		backups, err := bk.GetBackups()
		if err != nil {
			log.Fatal(err)
		}
		if cfg.full {
			if len(backups) <= number {
				fmt.Printf("Have only %v backups.\n", number)
			}
			left := number
			for _, b := range backups {
				if left == 1 {
					deleteBeforeTarget(b.Name, bk, cloud, true, backups, cfg.dryrun)
					return
				}
				dto := FetchSentinel(b.Name, bk, cloud)
				if !dto.IsIncremental() {
					left--
				}
			}
			fmt.Printf("Scanned all backups but didn't have %v full.", number)
		} else {
			if len(backups) <= number {
				fmt.Printf("Have only %v backups.\n", number)
			} else {
				cfg.target = backups[number-1].Name
				deleteBeforeTarget(cfg.target, bk, cloud, cfg.findFull, nil, cfg.dryrun)
			}
		}
	}
}

// HandleBackupList is invoked to perform wal-g backup-list
func HandleBackupList(cloud Cloud) {
	server := cloud.GetServer()
	var bk = &Backup{
		Prefix: cloud,
		Path:   GetBackupPath(server),
	}
	backups, err := bk.GetBackups()
	if err != nil {
		log.Fatal(err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	defer w.Flush()
	fmt.Fprintln(w, "name\tlast_modified\twal_segment_backup_start")

	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		fmt.Fprintln(w, fmt.Sprintf("%v\t%v\t%v", b.Name, b.Time.Format(time.RFC3339), b.WalFileName))
	}
}

// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(backupName string, cloud Cloud, dirArc string, mem bool) (lsn *uint64) {
	dirArc = ResolveSymlink(dirArc)
	lsn = deltaFetchRecursion(backupName, cloud, dirArc)

	if mem {
		f, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal(err)
		}

		pprof.WriteHeapProfile(f)
		defer f.Close()
	}
	return
}

// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursion(backupName string, cloud Cloud, dirArc string) (lsn *uint64) {
	server := cloud.GetServer()
	var bk *Backup
	// Check if BACKUPNAME exists and if it does extract to DIRARC.
	if backupName != "LATEST" {
		bk = &Backup{
			Prefix: cloud,
			Path:   GetBackupPath(server),
			Name:   aws.String(backupName),
		}
		bk.Js = aws.String(*bk.Path + *bk.Name + "_backup_stop_sentinel.json")

		exists, err := cloud.CheckExistence(&backupName)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		if !exists {
			log.Fatalf("Backup '%s' does not exist.\n", *bk.Name)
		}

		// Find the LATEST valid backup (checks against JSON file and grabs backup name) and extract to DIRARC.
	} else {
		bk = &Backup{
			Prefix: cloud,
			Path:   GetBackupPath(server),
		}

		latest, err := bk.GetLatest()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		bk.Name = aws.String(latest)
	}
	var dto = FetchSentinel(*bk.Name, bk, cloud)

	if dto.IsIncremental() {
		fmt.Printf("Delta from %v at LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN)
		deltaFetchRecursion(*dto.IncrementFrom, cloud, dirArc)
		fmt.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN, dto.LSN)
	}

	unwrapBackup(bk, dirArc, cloud, dto)

	lsn = dto.LSN
	return
}

// Do the job of unpacking Backup object
func unwrapBackup(bk *Backup, dirArc string, cloud Cloud, sentinel TarBallSentinelDto) {

	incrementBase := path.Join(dirArc, "increment_base")
	if !sentinel.IsIncremental() {
		var empty = true
		searchLambda := func(path string, info os.FileInfo, err error) error {
			if path != dirArc {
				empty = false
			}
			return nil
		}
		filepath.Walk(dirArc, searchLambda)

		if !empty {
			log.Fatalf("Directory %v for delta base must be empty", dirArc)
		}
	} else {
		defer func() {
			err := os.RemoveAll(incrementBase)
			if err != nil {
				log.Fatal(err)
			}
		}()

		err := os.MkdirAll(incrementBase, os.FileMode(0777))
		if err != nil {
			log.Fatal(err)
		}

		files, err := ioutil.ReadDir(dirArc)
		if err != nil {
			log.Fatal(err)
		}

		for _, f := range files {
			objName := f.Name()
			if objName != "increment_base" {
				err := os.Rename(path.Join(dirArc, objName), path.Join(incrementBase, objName))
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		for fileName, fd := range sentinel.Files {
			if !fd.IsSkipped {
				continue
			}
			fmt.Printf("Skipped file %v\n", fileName)
			targetPath := path.Join(dirArc, fileName)
			// this path is only used for increment restoration
			incrementalPath := path.Join(incrementBase, fileName)
			err = MoveFileAndCreateDirs(incrementalPath, targetPath, fileName)
			if err != nil {
				log.Fatal(err, "Failed to move skipped file for "+targetPath+" "+fileName)
			}
		}

	}

	var allKeys []string
	var keys []string
	allKeys, err := cloud.GetKeys(bk.Name)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	keys = allKeys[:len(allKeys)-1] // TODO: WTF is going on?
	f := &FileTarInterpreter{
		NewDir:             dirArc,
		Sentinel:           sentinel,
		IncrementalBaseDir: incrementBase,
	}
	out := make([]ReaderMaker, len(keys))
	for i, key := range keys {
		s := cloud.GetReaderMaker(key)
		out[i] = s
	}
	// Extract all compressed tar members except `pg_control.tar.lz4` if WALG version backup.
	err = ExtractAll(f, out)
	if serr, ok := err.(*UnsupportedFileTypeError); ok {
		log.Fatalf("%v\n", serr)
	} else if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	match := re.FindString(*bk.Name)
	if match == "" || sentinel.IsIncremental() {
		// Extract pg_control last. If pg_control does not exist, program exits with error code 1.
		name := *bk.Path + *bk.Name + "/tar_partitions/pg_control.tar.lz4"
		pgControl := &Archive{
			Prefix:  cloud,
			Archive: aws.String(name),
		}

		exists, err := pgControl.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		if exists {
			sentinel := make([]ReaderMaker, 1)
			sentinel[0] = cloud.GetReaderMaker(name)
			err := ExtractAll(f, sentinel)
			if serr, ok := err.(*UnsupportedFileTypeError); ok {
				log.Fatalf("%v\n", serr)
			} else if err != nil {
				log.Fatalf("%+v\n", err)
			}
			fmt.Printf("\nBackup extraction complete.\n")
		} else {
			log.Fatal("Corrupt backup: missing pg_control")
		}
	}
}

func GetDeltaConfig() (maxDeltas int, fromFull bool) {
	stepsStr, hasSteps := os.LookupEnv("WALG_DELTA_MAX_STEPS")
	var err error
	if hasSteps {
		maxDeltas, err = strconv.Atoi(stepsStr)
		if err != nil {
			log.Fatal("Unable to parse WALG_DELTA_MAX_STEPS ", err)
		}
	}
	origin, hasOrigin := os.LookupEnv("WALG_DELTA_ORIGIN")
	if hasOrigin {
		switch origin {
		case "LATEST":
		case "LATEST_FULL":
			fromFull = false
		default:
			log.Fatal("Unknown WALG_DELTA_ORIGIN:", origin)
		}
	}
	return
}

// HandleBackupPush is invoked to performa wal-g backup-push
func HandleBackupPush(dirArc string, tu TarUploaderInterface, cloud Cloud) {
	dirArc = ResolveSymlink(dirArc)
	server := cloud.GetServer()
	var bk = &Backup{
		Prefix: cloud,
		Path:   GetBackupPath(server),
	}

	cloud.BackupPush(dirArc, bk)
}

// HandleWALFetch is invoked to performa wal-g wal-fetch
func HandleWALFetch(cloud Cloud, walFileName string, location string, triggerPrefetch bool) {
	location = ResolveSymlink(location)
	if triggerPrefetch {
		defer forkPrefetch(walFileName, location)
	}

	_, _, running, prefetched := getPrefetchLocations(path.Dir(location), walFileName)
	seenSize := int64(-1)

	for {
		if stat, err := os.Stat(prefetched); err == nil {
			if stat.Size() != int64(WalSegmentSize) {
				log.Println("WAL-G: Prefetch error: wrong file size of prefetched file ", stat.Size())
				break
			}

			err = os.Rename(prefetched, location)
			if err != nil {
				log.Fatalf("%+v\n", err)
			}

			err := checkWALFileMagic(location)
			if err != nil {
				log.Println("Prefetched file contain errors", err)
				os.Remove(location)
				break
			}

			return
		} else if !os.IsNotExist(err) {
			log.Fatalf("%+v\n", err)
		}

		// We have race condition here, if running is renamed here, but it's OK

		if runStat, err := os.Stat(running); err == nil {
			observedSize := runStat.Size() // If there is no progress in 50 ms - start downloading myself
			if observedSize <= seenSize {
				defer func() {
					os.Remove(running) // we try to clean up and ignore here any error
					os.Remove(prefetched)
				}()
				break
			}
			seenSize = observedSize
		} else if os.IsNotExist(err) {
			break // Normal startup path
		} else {
			break // Abnormal path. Permission denied etc. Yes, I know that previous 'else' can be eliminated.
		}
		time.Sleep(50 * time.Millisecond)
	}

	DownloadWALFile(cloud, walFileName, location)
}

func checkWALFileMagic(prefetched string) error {
	file, err := os.Open(prefetched)
	if err != nil {
		return err
	}
	defer file.Close()
	magic := make([]byte, 4)
	file.Read(magic)
	if binary.LittleEndian.Uint32(magic) < 0xD061 {
		return errors.New("WAL-G: WAL file magic is invalid ")
	}

	return nil
}

// DownloadWALFile downloads a file and writes it to local file
func DownloadWALFile(cloud Cloud, walFileName string, location string) {
	server := cloud.GetServer()

	a := &Archive{
		Prefix:  cloud,
		Archive: aws.String(SanitizePath(*server + "/wal_005/" + walFileName + ".lzo")),
	}
	// Check existence of compressed LZO WAL file
	exists, err := a.CheckExistence()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	var crypter = OpenPGPCrypter{}
	if exists {
		arch, err := a.GetArchive()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		if crypter.IsUsed() {
			var reader io.Reader
			reader, err = crypter.Decrypt(arch)
			if err != nil {
				log.Fatalf("%v\n", err)
			}
			arch = ReadCascadeClose{reader, arch}
		}

		f, err := os.Create(location)
		if err != nil {
			log.Fatalf("%v\n", err)
		}

		err = DecompressLzo(f, arch)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		f.Close()
	} else if !exists {
		// Check existence of compressed LZ4 WAL file
		a.Archive = aws.String(SanitizePath(*server + "/wal_005/" + walFileName + ".lz4"))
		exists, err = a.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		if exists {
			arch, err := a.GetArchive()
			if err != nil {
				log.Fatalf("%+v\n", err)
			}

			if crypter.IsUsed() {
				var reader io.Reader
				reader, err = crypter.Decrypt(arch)
				if err != nil {
					log.Fatalf("%v\n", err)
				}
				arch = ReadCascadeClose{reader, arch}
			}

			f, err := os.OpenFile(location, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
			if err != nil {
				log.Fatalf("%v\n", err)
			}

			size, err := DecompressLz4(f, arch)
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			if size != int64(WalSegmentSize) {
				log.Fatal("Download WAL error: wrong size ", size)
			}
			err = f.Close()
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
		} else {
			log.Printf("Archive '%s' does not exist.\n", walFileName)
		}
	}
}

// HandleWALPush is invoked to perform wal-g wal-push
func HandleWALPush(tu TarUploaderInterface, dirArc string, cloud Cloud, verify bool) {
	bu := BgUploader{}
	// Look for new WALs while doing main upload
	bu.Start(dirArc, int32(GetMaxUploadConcurrency(16)-1), tu, cloud, verify)

	UploadWALFile(tu, dirArc, cloud, verify)

	bu.Stop()
}

// UploadWALFile from FS to the cloud
func UploadWALFile(tu TarUploaderInterface, dirArc string, cloud Cloud, verify bool) {
	path, err := tu.UploadWal(dirArc, cloud, verify)
	if re, ok := err.(Lz4Error); ok {
		log.Fatalf("FATAL: could not upload '%s' due to compression error.\n%+v\n", path, re)
	} else if err != nil {
		log.Printf("upload: could not upload '%s'\n", path)
		log.Fatalf("FATAL%+v\n", err)
	}
}
