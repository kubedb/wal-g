package mongo

import (
	"context"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
)

func HandleOplogReplay(ctx context.Context,
	since,
	until models.Timestamp,
	fetcher stages.BetweenFetcher,
	applier stages.Applier) error {
	tracelog.InfoLogger.Printf("Since: %s, Until: %s", since, until)
	return binary.HandleOplogReplay(ctx, since, until, fetcher, applier)
}

func RunOplogReplay(ctx context.Context, mongodbURL string, replayArgs binary.ReplyOplogConfig) error {
	tracelog.InfoLogger.Printf("URL: %s, DBNode= %s, Since: %s, Until: %s, ", mongodbURL, replayArgs.DBNode, replayArgs.Since, replayArgs.Until)
	return binary.RunOplogReplay(ctx, mongodbURL, replayArgs)
}
