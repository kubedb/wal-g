package main

import (
	"flag"
	"fmt"
	"github.com/wal-g/wal-g"
	"log"
	"os"
	"runtime/pprof"

	_ "github.com/wal-g/wal-g/s3"
)

var profile bool
var mem bool
var help bool
var l *log.Logger
var helpMsg = "  backup-fetch\tfetch a backup from S3\n" +
	"  backup-push\tstarts and uploads a finished backup to S3\n" +
	"  backup-list\tprints available backups\n" +
	"  wal-fetch\tfetch a WAL file from S3\n" +
	"  wal-push\tupload a WAL file to S3\n" +
	"  delete\tclear old backups and WALs\n"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of WAL-G:\n")
		fmt.Fprintf(os.Stderr, "%s", helpMsg)
		flag.PrintDefaults()
	}
	flag.BoolVar(&profile, "p", false, "\tProfiler (false by default)")
	flag.BoolVar(&mem, "m", false, "\tMemory profiler (false by default)")

	// this is temp solution to pass everything through flag. Will remove it when useing CLI like cobra or cli
	flag.BoolVar(&showVersion, "version", false, "\tversion")
	flag.BoolVar(&showVersion, "v", false, "\tversion")
	flag.BoolVar(&showVersionVerbose, "version-verbose", false, "\tLong version")
	flag.BoolVar(&showVersionVerbose, "vv", false, "\tLong version")

	l = log.New(os.Stderr, "", 0)
}

var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var showVersion bool
var showVersionVerbose bool

func main() {
	flag.Parse()

	if WalgVersion == "" {
		WalgVersion = "devel"
	}

	if showVersionVerbose {
		fmt.Println(WalgVersion, "\t", GitRevision, "\t", BuildDate)
		return
	}
	if showVersion {
		fmt.Println(WalgVersion)
		return
	}

	all := flag.Args()
	if len(all) < 1 {
		l.Fatalf("Please choose a command:\n%s", helpMsg)
	}
	command := all[0]
	firstArgument := ""
	if len(all) > 1 {
		firstArgument = all[1]
	}

	// Usage strings for supported commands
	// TODO: refactor arg parsing towards gloang flag usage and more helpful messages
	if firstArgument == "-h" || firstArgument == "--help" || (firstArgument == "" && command != "backup-list") {
		switch command {
		case "backup-fetch":
			fmt.Printf("usage:\twal-g backup-fetch output_directory backup_name\n\twal-g backup-fetch output_directory LATEST\n\n")
			os.Exit(1)
		case "backup-push":
			fmt.Printf("usage:\twal-g backup-push backup_directory\n\n")
			os.Exit(1)
		case "backup-list":
			fmt.Printf("usage:\twal-g backup-list\n\n")
			os.Exit(1)
		case "wal-fetch":
			fmt.Printf("usage:\twal-g wal-fetch wal_name file_name\n\t   wal_name: name of WAL archive\n\t   file_name: name of file to be written to\n\n")
			os.Exit(1)
		case "wal-push":
			fmt.Printf("usage:\twal-g wal-push archive_path\n\n")
			os.Exit(1)
		case "delete":
			fmt.Println(walg.DeleteUsage)
			os.Exit(1)
		default:
			l.Fatalf("Command '%s' is unsupported by WAL-G.\n\n", command)
		}
	}

	var backupName string
	var verify = false
	if len(all) == 3 {
		backupName = all[2]
		//TODO: use cobra
		verify = all[2] == "--verify"
	}

	// Various profiling options
	if profile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	waleCloudProvider := os.Getenv("WALE_CLOUD_PROVIDER")
	if waleCloudProvider == "" {
		log.Fatal(`Flag "WALE_CLOUD_PROVIDER" missing`)
	}

	operator, err := walg.NewOperator(waleCloudProvider)
	if err != nil {
		log.Fatal(err)
	}

	cloud, err := operator.GetCloud()
	if err != nil {
		log.Fatal(err)
	}

	tu, err := operator.GetTarUploader()
	if err != nil {
		log.Fatal(err)
	}

	if command == "wal-fetch" {
		// Fetch and decompress a WAL file from S3.
		walg.HandleWALFetch(cloud, firstArgument, backupName, true)
	} else if command == "wal-prefetch" {
		walg.HandleWALPrefetch(cloud, firstArgument, backupName)
	} else if command == "wal-push" {
		// Upload a WAL file to S3.
		walg.HandleWALPush(tu, firstArgument, cloud, verify)
	} else if command == "backup-push" {
		walg.HandleBackupPush(firstArgument, tu, cloud)
	} else if command == "backup-fetch" {
		walg.HandleBackupFetch(backupName, cloud, firstArgument, mem)
	} else if command == "backup-list" {
		walg.HandleBackupList(cloud)
	} else if command == "delete" {
		walg.HandleDelete(cloud, all)
	} else {
		l.Fatalf("Command '%s' is unsupported by WAL-G.", command)
	}
}
