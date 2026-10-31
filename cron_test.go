package gjob

import (
	"errors"
	"testing"
	"time"
)

func TestCronNextBasic(t *testing.T) {
	schedule, err := ParseSchedule("*/15 * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	from := time.Date(2026, 10, 31, 12, 7, 30, 0, time.UTC)
	next := schedule.Next(from, time.UTC)
	if want := time.Date(2026, 10, 31, 12, 15, 0, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

// The expression is evaluated in the job's zone, so the same wall-clock time fires
// at a different instant per zone.
func TestCronHonoursTimeZone(t *testing.T) {
	schedule, _ := ParseSchedule("0 9 * * *")

	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Skipf("zone data unavailable: %v", err)
	}

	from := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	next := schedule.Next(from, shanghai)
	if next.Hour() != 9 || next.Location() != shanghai {
		t.Fatalf("next = %s, want 09:00 in %s", next, shanghai)
	}
	// 09:00 CST is 01:00 UTC.
	if next.UTC().Hour() != 1 {
		t.Fatalf("next UTC hour = %d, want 1", next.UTC().Hour())
	}
}

func TestCronNamedFieldsAndRanges(t *testing.T) {
	schedule, err := ParseSchedule("0 9 * * MON-FRI")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	from := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC) // Saturday
	next := schedule.Next(from, time.UTC)
	if next.Weekday() != time.Monday {
		t.Fatalf("next = %s (%s), want Monday", next, next.Weekday())
	}
}

// POSIX: when both day fields are restricted, either match fires.
func TestCronDayOfMonthOrDayOfWeek(t *testing.T) {
	schedule, err := ParseSchedule("0 0 1 * MON")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	from := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	next := schedule.Next(from, time.UTC)
	if next.Day() != 1 && next.Weekday() != time.Monday {
		t.Fatalf("next = %s, matches neither rule", next)
	}
}

func TestCronRejectsSixFields(t *testing.T) {
	if _, err := ParseSchedule("0 0 9 * * *"); !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
	if _, err := ParseSchedule("bad"); !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
	if _, err := ParseSchedule("70 * * * *"); !errors.Is(err, ErrPermanent) {
		t.Fatalf("out-of-range minute err = %v, want ErrPermanent", err)
	}
}

// A leap-day expression must be found without scanning every minute.
func TestCronFindsLeapDay(t *testing.T) {
	schedule, err := ParseSchedule("0 0 29 2 *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	from := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	next := schedule.Next(from, time.UTC)
	if next.Year() != 2028 || next.Month() != time.February || next.Day() != 29 {
		t.Fatalf("next = %s, want 2028-02-29", next)
	}
}

func TestBackoffGrowsAndJitters(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 5, Initial: time.Second, Max: 8 * time.Second}
	cases := map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
		5: 8 * time.Second, // clamped
	}
	for attempt, want := range cases {
		if got := Backoff(policy, attempt, nil); got != want {
			t.Errorf("Backoff(%d) = %s, want %s", attempt, got, want)
		}
	}

	if !ShouldRetry(policy, 4) {
		t.Error("attempt 4 of 5 must retry")
	}
	if ShouldRetry(policy, 5) {
		t.Error("attempt 5 of 5 must not retry")
	}
	if ShouldRetry(RetryPolicy{}, 1) {
		t.Error("a zero policy must not retry")
	}
}

func TestStateTransitions(t *testing.T) {
	if err := checkTransition(StatePending, StateRunning); err != nil {
		t.Fatalf("pending -> running: %v", err)
	}
	if err := checkTransition(StateSucceeded, StateRunning); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("succeeded -> running err = %v, want ErrInvalidState", err)
	}
	if err := checkTransition(StateLost, StateRunning); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("lost -> running err = %v, want ErrInvalidState", err)
	}
	if !StateSucceeded.Terminal() || StateLost.Terminal() {
		t.Fatal("terminal classification is wrong")
	}
}
