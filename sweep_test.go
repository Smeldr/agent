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
	called := make(chan struct{}, 1)
	s, err := NewSweepScheduler("@every 50ms", "", func(_ context.Context) (int, int, error) {
		select {
		case called <- struct{}{}:
		default:
		}
		return 0, 0, nil
	})
	if err != nil {
		t.Fatalf("NewSweepScheduler: %v", err)
	}
	s.Start()
	defer s.Stop()

	select {
	case <-called:
		// scheduler fired at least once
	case <-time.After(2 * time.Second):
		t.Error("sweep was not called within 2s")
	}
}

// ——— NewEvalQueueScheduler ————————————————————————————————————————————————

type mockEvalQueue struct {
	calls atomic.Int32
}

func (m *mockEvalQueue) DrainEvalQueue(_ context.Context) (int, int, error) {
	m.calls.Add(1)
	return 1, 0, nil
}

func TestNewEvalQueueScheduler_valid(t *testing.T) {
	app := &mockEvalQueue{}
	sch, err := NewEvalQueueScheduler("", "UTC", app)
	if err != nil {
		t.Fatalf("NewEvalQueueScheduler: %v", err)
	}
	sch.Start()
	sch.Stop()
}

func TestNewEvalQueueScheduler_invalidTZ(t *testing.T) {
	app := &mockEvalQueue{}
	_, err := NewEvalQueueScheduler("*/5 * * * *", "Not/AZone", app)
	if err == nil {
		t.Fatal("expected error for invalid timezone, got nil")
	}
}

func TestNewEvalQueueScheduler_drains(t *testing.T) {
	app := &mockEvalQueue{}
	sch, err := NewEvalQueueScheduler("@every 20ms", "", app)
	if err != nil {
		t.Fatalf("NewEvalQueueScheduler: %v", err)
	}
	sch.Start()
	defer sch.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if app.calls.Load() >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if app.calls.Load() == 0 {
		t.Errorf("DrainEvalQueue was not called within 2s")
	}
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
