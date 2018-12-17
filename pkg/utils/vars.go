package utils

import (
	"context"

	"golang.org/x/sync/semaphore"
)

var (
	GCChan        chan string
	GCClearChunks chan string
	sem           = semaphore.NewWeighted(UploadConcurrency())
	ctx           = context.TODO()
)

func AcqureSem(num int64) {
	sem.Acquire(ctx, num)
}

func ReleaseSem(num int64) {
	sem.Release(num)
}
