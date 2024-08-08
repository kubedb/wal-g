package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupPush(dbnames []string, updateLatest, copyOnly bool) {
	ctx, cancel := context.WithCancel(context.Background())
	signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
	defer func() { _ = signalHandler.Close() }()

	storage, err := internal.ConfigureStorage()
	tracelog.ErrorLogger.FatalOnError(err)

	db, err := getSQLServerConnection()
	tracelog.ErrorLogger.FatalfOnError("failed to connect to SQLServer: %v", err)

	dbnames, err = getDatabasesToBackup(db, dbnames)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.ErrorLogger.FatalfOnError("failed to list databases to backup: %v", err)

	lock, err := RunOrReuseProxy(ctx, cancel, storage.RootFolder())
	tracelog.ErrorLogger.FatalOnError(err)
	defer lock.Close()

	server, _ := os.Hostname()
	timeStart := utility.TimeNowCrossPlatformLocal()
	var backupName string
	var sentinel *SentinelDto
	if updateLatest {
		backup, err := internal.GetBackupByName(internal.LatestString, utility.BaseBackupPath, storage.RootFolder())
		tracelog.ErrorLogger.FatalfOnError("can't find latest backup: %v", err)
		backupName = backup.Name
		sentinel = new(SentinelDto)
		err = backup.FetchSentinel(sentinel)
		tracelog.ErrorLogger.FatalOnError(err)
		sentinel.Databases = uniq(append(sentinel.Databases, dbnames...))
	} else {
		backupName = generateDatabaseBackupName()
		sentinel = &SentinelDto{
			Server:         server,
			Databases:      dbnames,
			StartLocalTime: timeStart,
		}
	}
	builtinCompression := blob.UseBuiltinCompression()
	err = runParallel(func(i int) error {
		return backupSingleDatabase(ctx, db, backupName, dbnames[i], builtinCompression, copyOnly)
	}, len(dbnames), getDBConcurrency())
	tracelog.ErrorLogger.FatalfOnError("overall backup failed: %v", err)

	if !updateLatest {
		sentinel.StopLocalTime = utility.TimeNowCrossPlatformLocal()
	}
	uploader := internal.NewRegularUploader(nil, storage.RootFolder().GetSubFolder(utility.BaseBackupPath))
	tracelog.InfoLogger.Printf("uploading sentinel: %s", sentinel)
	err = internal.UploadSentinel(uploader, sentinel, backupName)
	tracelog.ErrorLogger.FatalfOnError("failed to save sentinel: %v", err)

	tracelog.InfoLogger.Printf("backup finished")
}

func backupSingleDatabase(ctx context.Context, db *sql.DB, backupName string, dbname string, builtinCompression, copyOnlyBackup bool) error {
	baseURL := getDatabaseBackupURL(backupName, dbname)
	size, blobCount, err := estimateDBSize(db, dbname)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("database [%s] size is %d, required blob count %d", dbname, size, blobCount)
	urls := buildBackupUrls(baseURL, blobCount)
	sql := fmt.Sprintf("BACKUP DATABASE %s TO %s", quoteName(dbname), urls)
	sql += fmt.Sprintf(" WITH FORMAT, MAXTRANSFERSIZE=%d", MaxTransferSize)
	if builtinCompression {
		sql += ", COMPRESSION"
	}
	if copyOnlyBackup {
		sql += ", COPY_ONLY"
	}
	tracelog.InfoLogger.Printf("starting backup database [%s] to %s", dbname, urls)
	tracelog.DebugLogger.Printf("SQL: %s", sql)
	_, err = db.ExecContext(ctx, sql)
	if err != nil {
		tracelog.ErrorLogger.Printf("database [%s] backup failed: %#v", dbname, err)
	} else {
		tracelog.InfoLogger.Printf("database [%s] backup successfully finished", dbname)
	}
	return err
}
