package agent

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSweepScheduler_InvalidTimezone(t *testing.T) {
	_, err := NewSweepScheduler("* * * * *", "Not/AZone", func(_ context.Context) (int, int, error) {
		return 0, 0, nil
	})
	if err == nil {
		t.Fatal("want error for invalid timezone, got nil")
	}
}

func TestNewSweepScheduler_InvalidSchedule(t *testing.T) {
	_, err := NewSweepScheduler("not-a-cron", "UTC", func(_ context.Context) (int, int, error) {
		return 0, 0, nil
	})
	if err == nil {
		t.Fatal("want error for invalid cron expression, got nil")
	}
}

func TestSweepScheduler_StopGraceful(t *testing.T) {
	s, err := NewSweepScheduler("0 0 * * *", "UTC", func(_ context.Context) (int, int, error) {
		return 0, 0, nil
	})
	if err != nil {
		t.Fatalf("NewSweepScheduler: %v", err)
	}
	s.Start()
	s.Stop() // must not hang or panic
}

func TestSweepScheduler_Runs(t *testing.T) {
	var calls atomic.Int32
	s, err := NewSweepScheduler("@every 50ms", "", func(_ context.Context) (int, int, error) {
		calls.Add(1)
		return 0, 0, nil
	})
	if err != nil {
		t.Fatalf("NewSweepScheduler: %v", err)
	}
	s.Start()
	defer s.Stop()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if calls.Load() >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("sweep was not called within 500ms (calls=%d)", calls.Load())
}

func TestSweepScheduler_SingletonMode(t *testing.T) {
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	s, err := NewSweepScheduler("@every 50ms", "", func(_ context.Context) (int, int, error) {
		c := concurrent.Add(1)
		for {
			cur := maxConcurrent.Load()
			if c <= cur || maxConcurrent.CompareAndSwap(cur, c) {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
		concurrent.Add(-1)
		return 1, 0, nil
	})
	if err != nil {
		t.Fatalf("NewSweepScheduler: %v", err)
	}
	s.Start()
	time.Sleep(300 * time.Millisecond)
	s.Stop()

	if maxConcurrent.Load() > 1 {
		t.Errorf("want max 1 concurrent run, got %d", maxConcurrent.Load())
	}
}
