package mongo

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"kubeops.dev/sidekick/apis/apps/v1alpha1"
	storageapi "kubestash.dev/apimachinery/apis/storage/v1alpha1"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"k8s.io/klog/v2/klogr"
	archiverv1alpha1 "kubedb.dev/apimachinery/apis/archiver/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	runtime_client "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	componentCheckElapsedTime = 5 * time.Second
)

type Retention struct {
	kubeClient      *kubernetes.Clientset
	client          runtime_client.Client
	logger          logr.Logger
	logBackupOption *archiverv1alpha1.LogBackupOptions
	backend         *storageapi.Backend
	snapshot        *storageapi.Snapshot
	backupStorage   *storageapi.BackupStorage
	mu              sync.Mutex
	sidekick        *v1alpha1.Sidekick
}

func GetNewRetention(ctx context.Context, snapshotRef *metav1.ObjectMeta) (*Retention, error) {
	var err error
	rt := &Retention{}
	ctrl.SetLogger(klogr.New()) // nolint:staticcheck
	rt.logger = ctrl.Log.WithName("wal-log deletion")
	if err = setClientToRetention(rt); err != nil {
		return nil, fmt.Errorf("failed to set client to rt: %w", err)
	}
	podName, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	rt.sidekick, err = rt.getSidekick(ctx, podName, os.Getenv("NAMESPACE"))
	if err != nil {
		return nil, fmt.Errorf("failed to get sidekick: %w", err)
	}

	rt.snapshot, err = rt.getSnapshot(ctx, snapshotRef)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	rt.logBackupOption, err = rt.getLogBackupOption()
	if err != nil {
		return nil, fmt.Errorf("failed to get logBackupOption: %w", err)
	}

	rt.backend, err = rt.getBackend()
	if err != nil {
		return nil, fmt.Errorf("failed to get backupStorage backend: %w", err)
	}

	rt.backupStorage = rt.createBackupStorage()

	return rt, nil
}

func (rt *Retention) getSidekick(ctx context.Context, sidekickname string, sidekicknamespace string) (*v1alpha1.Sidekick, error) {
	sk := &v1alpha1.Sidekick{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sidekickname,
			Namespace: sidekicknamespace,
		},
	}

	err := rt.client.Get(ctx, runtime_client.ObjectKeyFromObject(sk), sk)
	if err != nil {
		return nil, err
	}

	return sk, nil
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
	_, err = rcron.AddFunc(rt.logBackupOption.RetentionSchedule, func() {
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
	purger, err := archive.NewStoragePurger(archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}
	purger.SetNodeSpecificPurger(pushArgs.dbNode)
	count, err := mongo.HandleOplogPurgeForKubeDB(downloader, purger, pitrAfterTime, dryRun)
	klog.Infof("Deleted Count: %d", count)
	compName := "wal"
	compName = compName + "-" + pushArgs.dbNode

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

func (rt *Retention) getLogBackupOption() (*archiverv1alpha1.LogBackupOptions, error) {
	extraArgs := rt.sidekick.Spec.ExtraArgs
	var lbo archiverv1alpha1.LogBackupOptions
	val, err := archiverv1alpha1.GetValueFromExtraArgs(extraArgs, archiverv1alpha1.ExtraArgsKeyLogBackupOpt, &lbo)
	if err != nil {
		return nil, err
	}
	logbackup, ok := val.(*archiverv1alpha1.LogBackupOptions)
	if !ok {
		return nil, fmt.Errorf("unexpected type for log backup options")
	}
	return logbackup, nil
}

func (rt *Retention) getBackend() (*storageapi.Backend, error) {
	extraArgs := rt.sidekick.Spec.ExtraArgs
	var be storageapi.Backend
	val, err := archiverv1alpha1.GetValueFromExtraArgs(extraArgs, archiverv1alpha1.ExtraArgsKeyStorage, &be)
	if err != nil {
		return nil, err
	}
	storage, ok := val.(*storageapi.Backend)
	if !ok {
		return nil, fmt.Errorf("unexpected type for log backup options")
	}
	return storage, nil
}

func (rt *Retention) createBackupStorage() *storageapi.BackupStorage {
	bs := &storageapi.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backupStorage",
			Namespace: os.Getenv("NAMESPACE"),
		},
		Spec: storageapi.BackupStorageSpec{
			Storage: *rt.backend,
		},
	}

	return bs
}

func (rt *Retention) pitrDiscoveryAfterTime() (*time.Time, error) {
	pitrAfterTime, err := archiverv1alpha1.ParseCutoffTimeFromPeriod(rt.logBackupOption.RetentionPeriod, time.Now())
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
