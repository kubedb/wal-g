package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const folderListShortDescription = "Prints objects in the provided storage folder"
const recursiveFlag = "recursive"
const recursiveShortHand = "r"
const allVersionsFlag = "all-versions"
const jsonFlag = "json"

// folderListCmd represents the folderList command
var folderListCmd = &cobra.Command{
	Use:   "ls [relative folder path]",
	Short: folderListShortDescription,
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		var path string
		if len(args) > 0 {
			path = args[0]
		} else {
			path = ""
		}

		err := exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			if showAllVersions {
				storage.SetShowAllVersions(folder, true)
			}
			if glob {
				if jsonOutput {
					return storagetools.HandleFolderListWithGlobJSON(folder, path, recursive)
				}
				return storagetools.HandleFolderListWithGlob(folder, path, recursive)
			}
			subfolder := folder.GetSubFolder(path)
			if jsonOutput {
				return storagetools.HandleFolderListJSON(subfolder, recursive)
			}
			return storagetools.HandleFolderList(subfolder, recursive)
		})
		if err != nil {
			tracelog.ErrorLogger.FatalOnError(err)
		}
	},
}

var recursive bool
var showAllVersions bool
var jsonOutput bool

func init() {
	folderListCmd.Flags().BoolVarP(&recursive, recursiveFlag, recursiveShortHand, false, "List folder recursively")
	folderListCmd.Flags().BoolVar(&showAllVersions, allVersionsFlag, false, "Show all object versions including deleted (S3 with versioning)")
	folderListCmd.Flags().BoolVar(&jsonOutput, jsonFlag, false, "Print the listing as a JSON array")
	StorageToolsCmd.AddCommand(folderListCmd)
}
