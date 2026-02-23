package mongo

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	v1 "kubedb.dev/apimachinery/apis/kubedb/v1"
	storageapi "kubestash.dev/apimachinery/apis/storage/v1alpha1"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	archiverv1alpha1 "kubedb.dev/apimachinery/apis/archiver/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	runtime_client "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	componentCheckElapsedTime = 5 * time.Second
)

type Retention struct {
	kubeClient    *kubernetes.Clientset
	client        runtime_client.Client
	logger        logr.Logger
	db            *v1.MongoDB
	snapshot      *storageapi.Snapshot
	archiver      *archiverv1alpha1.MongoDBArchiver
	backupStorage *storageapi.BackupStorage
	mu            sync.Mutex
}

func GetNewRetention(ctx context.Context, snapshotRef *metav1.ObjectMeta) (*Retention, error) {
	var err error
	rt := &Retention{}
	rt.logger = ctrl.Log.WithName("wal-log deletion")
	if err = setClientToRetention(rt); err != nil {
		return nil, fmt.Errorf("failed to set client to rt: %w", err)
	}
	rt.snapshot, err = rt.getSnapshot(ctx, snapshotRef)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	rt.db, err = rt.getDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get db: %w", err)
	}

	rt.archiver, err = rt.getArchiver(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get archiver: %w", err)
	}

	rt.backupStorage, err = rt.getBackupStorage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get backupStorage: %w", err)
	}

	return rt, nil
}

func (rt *Retention) Run(ctx context.Context) {
	compName := "wal"
	dbNode, err := conf.GetRequiredSetting(conf.MongoDBNode)
	if err != nil {
		return
	}
	compName = compName + "-" + dbNode

	snapshot, err := rt.waitUntilWalComponentExist(ctx, compName)
	if err != nil {
		rt.logger.Error(err, "failed waiting for wal component")
		return
	}

	components := snapshot.Status.Components

	if components[compName].LogStats.LastLogRetentionStats == nil {
		rt.logger.Info("Starting the first wal deletion.")
		if err := rt.runRetention(ctx); err != nil {
			rt.logger.Error(err, "failed to run retention")
		}
	}

	rcron := cron.New()
	_, err = rcron.AddFunc(rt.archiver.Spec.LogBackup.RetentionSchedule, func() {
		err := rt.runRetention(ctx)
		if err != nil {
			rt.logger.Error(err, "failed to run retention")
		}
	})
	if err != nil {
		rt.logger.Error(err, "failed to add retention cron job")
	}
	rcron.Start()
}

func (rt *Retention) runRetention(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	pitrAfterTime, err := rt.pitrDiscoveryAfterTime()
	if err != nil {
		return err
	}
	return rt.runOplogPurgeForKubeDB(ctx, pitrAfterTime, false)
}

var (
	confirmedOplogPurge bool
)

// oplogPurgeCmd represents the delete command
var oplogPurgeCmd = &cobra.Command{
	Use:   "oplog-purge",
	Short: "Purges oplog archives",
	Run:   runOplogPurge,
}

func pitrDiscoveryAfterTime() *time.Time {
	pitrDur, err := conf.GetOplogPITRDiscoveryIntervalSetting()
	tracelog.ErrorLogger.FatalOnError(err)
	if pitrDur == nil {
		return nil
	}

	pitrAfterTime := time.Now().Add(-*pitrDur)
	return &pitrAfterTime
}

func runOplogPurge(cmd *cobra.Command, args []string) {
	pitrAfterTime := pitrDiscoveryAfterTime()
	// set up storage downloader client
	downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
	tracelog.ErrorLogger.FatalOnError(err)

	// set up storage purger client
	purger, err := archive.NewStoragePurger(archive.NewDefaultStorageSettings())
	tracelog.ErrorLogger.FatalOnError(err)

	err = mongo.HandleOplogPurge(downloader, purger, pitrAfterTime, !confirmedOplogPurge)
	tracelog.ErrorLogger.FatalOnError(err)
}
func (rt *Retention) runOplogPurgeForKubeDB(ctx context.Context, pitrAfterTime *time.Time, dryRun bool) error {
	pushArgs, err := buildOplogPushRunArgs()
	if err != nil {
		return err
	}
	downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}
	downloader.SetNodeSpecificDownloader(pushArgs.dbNode)
	klog.Infof("%s", pushArgs.dbNode)
	purger, err := archive.NewStoragePurger(archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}
	purger.SetNodeSpecificPurger(pushArgs.dbNode)
	count, err := mongo.HandleOplogPurgeForKubeDB(downloader, purger, pitrAfterTime, dryRun, rt.db.Name, rt.db.Namespace)
	klog.Infof("Deleted Count: %d", count)
	compName := "wal"

	if err == nil {
		updateRetentionStats(ctx, rt, int64(count), "", compName)
	} else {
		updateRetentionStats(ctx, rt, int64(count), err.Error(), compName)
	}

	return err
}

func init() {
	cmd.AddCommand(oplogPurgeCmd)
	oplogPurgeCmd.Flags().BoolVar(&confirmedOplogPurge, internal.ConfirmFlag, false, "Confirms oplog archives deletion")
}

func (rt *Retention) getSnapshot(ctx context.Context, ref *metav1.ObjectMeta) (*storageapi.Snapshot, error) {
	snapshot := &storageapi.Snapshot{
		ObjectMeta: *ref,
	}
	err := rt.client.Get(ctx, runtime_client.ObjectKeyFromObject(snapshot), snapshot)
	return snapshot, err
}

func (rt *Retention) getDB(ctx context.Context) (*v1.MongoDB, error) {
	db := &v1.MongoDB{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rt.snapshot.Spec.AppRef.Name,
			Namespace: rt.snapshot.Spec.AppRef.Namespace,
		},
	}
	err := rt.client.Get(ctx, runtime_client.ObjectKeyFromObject(db), db)
	return db, err
}

func (rt *Retention) getArchiver(ctx context.Context) (*archiverv1alpha1.MongoDBArchiver, error) {
	archiver := &archiverv1alpha1.MongoDBArchiver{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rt.db.Spec.Archiver.Ref.Name,
			Namespace: rt.db.Spec.Archiver.Ref.Namespace,
		},
	}
	err := rt.client.Get(ctx, runtime_client.ObjectKeyFromObject(archiver), archiver)
	return archiver, err
}

func (rt *Retention) getBackupStorage(ctx context.Context) (*storageapi.BackupStorage, error) {
	bs := &storageapi.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rt.archiver.Spec.BackupStorage.Ref.Name,
			Namespace: rt.archiver.Spec.BackupStorage.Ref.Namespace,
		},
	}
	err := rt.client.Get(ctx, runtime_client.ObjectKeyFromObject(bs), bs)
	return bs, err
}

func (rt *Retention) pitrDiscoveryAfterTime() (*time.Time, error) {
	pitrAfterTime, err := archiverv1alpha1.ParseCutoffTimeFromPeriod(rt.archiver.Spec.LogBackup.RetentionPeriod, time.Now())
	if err != nil {
		return nil, err
	}
	return &pitrAfterTime, nil
}

func (rt *Retention) waitUntilWalComponentExist(ctx context.Context, compName string) (*storageapi.Snapshot, error) {
	var err error
	var snapshot *storageapi.Snapshot
	ticker := time.NewTicker(componentCheckElapsedTime)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			rt.logger.Info("context cancelled, stopping wait")
			return nil, ctx.Err()
		case <-ticker.C:
			snapshot, err = rt.getSnapshot(ctx, &rt.snapshot.ObjectMeta)
			if err != nil {
				return nil, err
			}

			components := snapshot.Status.Components
			if components != nil {
				if comp, ok := components[compName]; ok {
					if comp.LogStats != nil {
						rt.logger.Info("WAL component found", "component", comp)
						return snapshot, nil
					}
				}
			}
			rt.logger.Info("Waiting for LogStats to be available...")
		}
	}
}
