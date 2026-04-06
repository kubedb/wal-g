package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
)

const backupPushShortDescription = "Creates new backup and pushes it to the storage"

var backupPushDatabases []string
var backupUpdateLatest bool
var backupCopyOnly bool

var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: backupPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()
		sqlserver.HandleBackupPush(backupPushDatabases, backupUpdateLatest, backupCopyOnly)
	},
}

func init() {
	backupPushCmd.PersistentFlags().StringSliceVarP(&backupPushDatabases, "databases", "d", []string{},
		"List of databases to backup. All not-system databases as default")
	backupPushCmd.PersistentFlags().BoolVarP(&backupUpdateLatest, "update-latest", "u", false,
		"Update latest backup instead of creating new one")
	backupPushCmd.PersistentFlags().BoolVar(&backupCopyOnly, "copy-only", false,
		"Run BACKUP DATABASE ... WITH COPY_ONLY (does not affect diff backup chain)")
	cmd.AddCommand(backupPushCmd)
}
