package walg

import "io"

type Cloud interface {
	GetServer() *string
	GetBucket() *string
	CheckExistence(*string) (bool, error)
	GetETag(*string) (*string, error)
	GetArchive(*string) (io.ReadCloser, error)
	BackupPush(string, *Backup)
	GetBackups(*Backup) ([]BackupTime, error)
	GetReaderMaker(string) ReaderMaker
	DeleteWALBefore(BackupTime, *Backup)
	DropBackup(*string)
	GetKeys(*string) ([]string, error)
}
