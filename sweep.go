package agent

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	gocron "github.com/go-co-op/gocron/v2"
)

// SweepFunc is the function called on each scheduled sweep run.
// Returns (walked, flagged, skipped int, err error) — matching
// smeldr.App.SweepStructural / smeldr.App.DrainEvalQueue (smeldr.dev/core
// v1.75.0+). walked is the total items examined this run — the count that
// makes flagged/skipped meaningful: without it, a clean run (flagged=0,
// skipped=0) can't be told apart from nothing having been checked at all.
type SweepFunc func(ctx context.Context) (walked, flagged, skipped int, err error)

// SweepScheduler runs a [SweepFunc] on a cron schedule using singleton mode
// ([gocron.LimitModeReschedule]) so overlapping runs are never started.
//
// Usage — wire Layer 3a structural sweep to run every hour:
//
//	sweep, err := agent.NewSweepScheduler("0 * * * *", "UTC", app.SweepStructural)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	sweep.Start()
//	defer sweep.Stop()
type SweepScheduler struct {
	s gocron.Scheduler
}

// NewSweepScheduler creates a SweepScheduler.
//   - schedule: 5-field cron expression, e.g. "0 * * * *" (every hour).
//   - timezone: IANA timezone, e.g. "Europe/Copenhagen". Empty defaults to UTC.
//   - sweep: the function to call on each tick.
func NewSweepScheduler(schedule, timezone string, sweep SweepFunc) (*SweepScheduler, error) {
	tz := timezone
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("sweep scheduler: invalid timezone %q: %w", tz, err)
	}
	s, err := gocron.NewScheduler(gocron.WithLocation(loc))
	if err != nil {
		return nil, fmt.Errorf("sweep scheduler: %w", err)
	}
	_, err = s.NewJob(
		gocron.CronJob(schedule, false),
		gocron.NewTask(func(ctx context.Context) {
			walked, flagged, skipped, runErr := sweep(ctx)
			if runErr != nil {
				slog.Error("sweep: structural sweep failed", "error", runErr)
				return
			}
			if walked == 0 && flagged == 0 && skipped == 0 {
				slog.Debug("sweep: structural sweep done", "walked", 0, "flagged", 0, "skipped", 0)
				return
			}
			// walked > 0 alone is informative even when flagged == 0 && skipped
			// == 0 — it's the difference between "checked everything, found
			// nothing" and "nothing to check" (T223).
			slog.Info("sweep: structural sweep done", "walked", walked, "flagged", flagged, "skipped", skipped)
		}),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	)
	if err != nil {
		return nil, fmt.Errorf("sweep scheduler: register job: %w", err)
	}
	return &SweepScheduler{s: s}, nil
}

// Start begins scheduling. Non-blocking.
func (s *SweepScheduler) Start() { s.s.Start() }

// Stop gracefully shuts down the scheduler, waiting for in-flight runs.
func (s *SweepScheduler) Stop() {
	if err := s.s.Shutdown(); err != nil {
		slog.Error("sweep scheduler: shutdown error", "error", err)
	}
}

// NewEvalQueueScheduler creates a [SweepScheduler] that drains
// [smeldr.App.DrainEvalQueue] on a cron schedule.
//
// Default schedule is "*/5 * * * *" (every 5 minutes). Pass an empty string to
// use the default.
//
// Usage:
//
//	sch, err := agent.NewEvalQueueScheduler("", "UTC", app)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	sch.Start()
//	defer sch.Stop()
func NewEvalQueueScheduler(schedule, timezone string, app interface {
	DrainEvalQueue(ctx context.Context) (walked, triggered, skipped int, err error)
}) (*SweepScheduler, error) {
	s := schedule
	if s == "" {
		s = "*/5 * * * *"
	}
	return NewSweepScheduler(s, timezone, func(ctx context.Context) (int, int, int, error) {
		return app.DrainEvalQueue(ctx)
	})
}
