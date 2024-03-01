package mongo

import (
	"context"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	"golang.org/x/sync/errgroup"
)

// HandleOplogReplay starts oplog replay process: download from storage and apply to mongodb
func HandleOplogReplay(ctx context.Context,
	since,
	until models.Timestamp,
	fetcher stages.BetweenFetcher,
	applier stages.Applier) error {
	since = models.Timestamp{1709198864., 1}
	until = models.Timestamp{1709199223, 1}
	tracelog.InfoLogger.Printf("Since: %s, Until: %s", since, until)

	errgrp, ctx := errgroup.WithContext(ctx)
	var errs []<-chan error

	oplogc, errc, err := fetcher.FetchBetween(ctx, since, until)
	if err != nil {
		return err
	}
	errs = append(errs, errc)

	errc, err = applier.Apply(ctx, oplogc)
	if err != nil {
		return err
	}
	errs = append(errs, errc)

	for _, errc := range errs {
		errc := errc
		errgrp.Go(func() error {
			return <-errc
		})
	}

	return errgrp.Wait()
}
