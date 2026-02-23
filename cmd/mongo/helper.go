package mongo

import (
	"context"
	"fmt"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	kmc "kmodules.xyz/client-go/client"
	"kubedb.dev/apimachinery/pkg/factory"
	storageapi "kubestash.dev/apimachinery/apis/storage/v1alpha1"
	runtime_client "sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func setClientToRetention(retention *Retention) error {
	k8sConfig, err := restclient.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in cluster config: %w", err)
	}
	retention.kubeClient, err = kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return fmt.Errorf("failed to get kubernetes client: %w", err)
	}

	retention.client, err = factory.NewUncachedClient(k8sConfig)

	if err != nil {
		return fmt.Errorf("failed to get runtime client: %w", err)
	}

	return nil
}

func updateRetentionStats(ctx context.Context, rt *Retention, deletedCount int64, errMsg string, ComponentWal string) error {
	snapshot, err := rt.getSnapshot(ctx, &rt.snapshot.ObjectMeta)
	if err != nil {
		return err
	}

	if snapshot.Status.Components == nil {
		snapshot.Status.Components = make(map[string]storageapi.Component)
	}

	comp, exists := snapshot.Status.Components[ComponentWal]
	if !exists {
		comp = storageapi.Component{}
	}

	if comp.LogStats == nil {
		comp.LogStats = &storageapi.LogStats{}
	}
	if comp.LogStats.LastLogRetentionStats == nil {
		comp.LogStats.LastLogRetentionStats = []storageapi.LogRetentionStatus{}
	}

	retention := storageapi.LogRetentionStatus{
		LastExecutionTime:      ptr(time.Now().Format(time.RFC3339)),
		RetentionPeriodApplied: rt.archiver.Spec.LogBackup.RetentionPeriod,
		DeletedLogCount:        deletedCount,
		Error:                  errMsg,
	}

	comp.LogStats.LastLogRetentionStats = append(comp.LogStats.LastLogRetentionStats,
		retention,
	)

	limit := int(rt.archiver.Spec.LogBackup.LogRetentionHistoryLimit)
	stats := comp.LogStats.LastLogRetentionStats
	if len(stats) > limit && limit > 0 {
		comp.LogStats.LastLogRetentionStats = stats[len(stats)-limit:] // keep latest `limit` entries
	}
	snapshot.Status.Components[ComponentWal] = comp
	err = patchRetentionStats(ctx, rt.client, snapshot, ComponentWal)
	if err != nil {
		return err
	}

	return nil
}

func patchRetentionStats(ctx context.Context, rClient runtime_client.Client, snapshot *storageapi.Snapshot, ComponentWal string) error {
	_, err := kmc.PatchStatus(
		ctx,
		rClient,
		snapshot,
		func(obj runtime_client.Object) runtime_client.Object {
			in := obj.(*storageapi.Snapshot)
			if snapshot.Status.Components != nil {
				if in.Status.Components == nil {
					in.Status.Components = make(map[string]storageapi.Component)
				}
				in.Status.Components[ComponentWal].LogStats.LastLogRetentionStats = snapshot.Status.Components[ComponentWal].LogStats.LastLogRetentionStats
			}
			return in
		},
	)
	return err
}

func ptr[T any](v T) *T {
	return &v
}
