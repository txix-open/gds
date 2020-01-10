package utils

import (
	"context"
	"time"
)

func Wait(ctx context.Context, condition func() bool, tick time.Duration) bool {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			result := condition()
			if result {
				return true
			}
		case <-ctx.Done():
			return false
		}
	}
}
