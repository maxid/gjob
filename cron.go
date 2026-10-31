package gjob

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed cron expression.
//
// Only the standard five fields are accepted (minute hour day-of-month month
// day-of-week). Two nodes must never disagree about how an expression is read, so a
// six-field expression with seconds is a configuration error rather than a silently
// different schedule.
type Schedule struct {
	minute, hour, dom, month, dow uint64
	// domStar and dowStar keep the POSIX rule: when both are restricted, a match on
	// either one fires.
	domStar bool
	dowStar bool
	spec    string
}

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dayNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// ParseSchedule parses a five-field cron expression.
func ParseSchedule(expression string) (*Schedule, error) {
	fields := strings.Fields(strings.TrimSpace(expression))
	if len(fields) != 5 {
		return nil, fmt.Errorf("%w: cron %q must have exactly 5 fields, got %d", ErrPermanent, expression, len(fields))
	}

	s := &Schedule{spec: expression}
	var err error
	if s.minute, err = parseField(fields[0], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("%w: cron %q minute: %v", ErrPermanent, expression, err)
	}
	if s.hour, err = parseField(fields[1], 0, 23, nil); err != nil {
		return nil, fmt.Errorf("%w: cron %q hour: %v", ErrPermanent, expression, err)
	}
	if s.dom, err = parseField(fields[2], 1, 31, nil); err != nil {
		return nil, fmt.Errorf("%w: cron %q day-of-month: %v", ErrPermanent, expression, err)
	}
	if s.month, err = parseField(fields[3], 1, 12, monthNames); err != nil {
		return nil, fmt.Errorf("%w: cron %q month: %v", ErrPermanent, expression, err)
	}
	if s.dow, err = parseField(fields[4], 0, 6, dayNames); err != nil {
		return nil, fmt.Errorf("%w: cron %q day-of-week: %v", ErrPermanent, expression, err)
	}

	s.domStar = fields[2] == "*"
	s.dowStar = fields[4] == "*"
	return s, nil
}

// String returns the original expression.
func (s *Schedule) String() string { return s.spec }

// Next returns the first firing time strictly after `from`, evaluated in loc.
//
// The search walks whole days and then whole hours, so a sparse expression such as
// "0 0 29 2 *" costs a bounded number of iterations across years rather than a
// minute-by-minute scan.
func (s *Schedule) Next(from time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	// Start at the next whole minute.
	cursor := from.In(loc).Truncate(time.Minute).Add(time.Minute)

	// Four years covers a leap day, the sparsest standard expression.
	limit := cursor.AddDate(5, 0, 0)
	for !cursor.After(limit) {
		if s.month&(1<<uint(cursor.Month())) == 0 {
			// Jump to the first day of the next month.
			cursor = time.Date(cursor.Year(), cursor.Month(), 1, 0, 0, 0, 0, loc).AddDate(0, 1, 0)
			continue
		}
		if !s.dayMatches(cursor) {
			cursor = time.Date(cursor.Year(), cursor.Month(), cursor.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, 1)
			continue
		}
		if s.hour&(1<<uint(cursor.Hour())) == 0 {
			cursor = time.Date(cursor.Year(), cursor.Month(), cursor.Day(), cursor.Hour(), 0, 0, 0, loc).Add(time.Hour)
			continue
		}
		if s.minute&(1<<uint(cursor.Minute())) == 0 {
			cursor = cursor.Add(time.Minute)
			continue
		}
		return cursor
	}
	return time.Time{}
}

// dayMatches applies the POSIX rule for day-of-month versus day-of-week.
func (s *Schedule) dayMatches(t time.Time) bool {
	domMatch := s.dom&(1<<uint(t.Day())) != 0
	dowMatch := s.dow&(1<<uint(int(t.Weekday()))) != 0

	switch {
	case s.domStar && s.dowStar:
		return true
	case s.domStar:
		return dowMatch
	case s.dowStar:
		return domMatch
	default:
		// Both restricted: POSIX fires when either matches.
		return domMatch || dowMatch
	}
}

// parseField parses a comma-separated field with ranges, steps and names.
func parseField(field string, min, max int, names map[string]int) (uint64, error) {
	var bits uint64

	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return 0, fmt.Errorf("empty element")
		}

		step := 1
		if idx := strings.Index(part, "/"); idx >= 0 {
			parsed, err := strconv.Atoi(part[idx+1:])
			if err != nil || parsed <= 0 {
				return 0, fmt.Errorf("invalid step %q", part[idx+1:])
			}
			step = parsed
			part = part[:idx]
		}

		var low, high int
		switch {
		case part == "*":
			low, high = min, max
		case strings.Contains(part, "-"):
			bounds := strings.SplitN(part, "-", 2)
			parsedLow, err := parseValue(bounds[0], names)
			if err != nil {
				return 0, err
			}
			parsedHigh, err := parseValue(bounds[1], names)
			if err != nil {
				return 0, err
			}
			low, high = parsedLow, parsedHigh
		default:
			parsed, err := parseValue(part, names)
			if err != nil {
				return 0, err
			}
			low = parsed
			high = parsed
			if step > 1 {
				// "5/10" means "from 5 to the field maximum, every 10".
				high = max
			}
		}

		if low < min || high > max || low > high {
			return 0, fmt.Errorf("value %q outside %d-%d", part, min, max)
		}
		for value := low; value <= high; value += step {
			bits |= 1 << uint(value)
		}
	}
	return bits, nil
}

func parseValue(raw string, names map[string]int) (int, error) {
	raw = strings.TrimSpace(raw)
	if names != nil {
		if value, ok := names[strings.ToLower(raw)]; ok {
			return value, nil
		}
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", raw)
	}
	return value, nil
}
