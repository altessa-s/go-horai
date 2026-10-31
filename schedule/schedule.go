// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// ErrInvalid reports a schedule spec that does not parse.
var ErrInvalid = errors.New("invalid schedule")

// Schedule describes the activation times of a parsed spec; [Parse]
// returns it. Implementations are safe for concurrent use.
type Schedule interface {
	// Next returns the first activation time strictly after t, in t's
	// location. The fields are evaluated in the zone of the spec's
	// "CRON_TZ=" or "TZ=" prefix, or in t's location when the spec has none.
	// It returns the zero [time.Time] when no activation can be found.
	Next(t time.Time) time.Time
}

// scheduleParser is the cron grammar of go-horai: six fields with leading
// seconds plus descriptors ("@every 5m", "@hourly", ...). A "CRON_TZ=" or
// "TZ=" prefix selects the time zone of the spec. A cron.Parser is a
// read-only value, safe for concurrent use.
var scheduleParser = cron.NewParser(
	cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// Parse parses spec using six space-separated fields with leading
// seconds ("0 */5 * * * *"), or a descriptor ("@every 5m", "@hourly",
// "@daily", ...), optionally prefixed with "CRON_TZ=<zone> " or
// "TZ=<zone> ". A spec that does not parse, the empty spec included, returns
// an error matching [ErrInvalid] that quotes spec.
func Parse(spec string) (schedule Schedule, err error) {
	if perr := checkZonePrefix(spec); perr != nil {
		return nil, fmt.Errorf("%w %q: %w", ErrInvalid, spec, perr)
	}
	defer func() {
		// The parser is third-party code fed with configuration: a spec it cannot handle must be an invalid spec, not a crash.
		if r := recover(); r != nil {
			schedule, err = nil, fmt.Errorf("%w %q: the cron parser failed: %v", ErrInvalid, spec, r)
		}
	}()
	schedule, err = scheduleParser.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %w", ErrInvalid, spec, err)
	}
	return schedule, nil
}

// errZoneWithoutSchedule rejects a time zone prefix that no space separates from a schedule; robfig/cron v3 slices such a spec
// out of range and panics.
var errZoneWithoutSchedule = errors.New(`a "CRON_TZ=" or "TZ=" prefix needs a space and a schedule after the zone`)

// checkZonePrefix checks that a "CRON_TZ=" or "TZ=" prefix of spec is followed by a space, as the parser requires.
func checkZonePrefix(spec string) error {
	if (strings.HasPrefix(spec, "TZ=") || strings.HasPrefix(spec, "CRON_TZ=")) && !strings.Contains(spec, " ") {
		return errZoneWithoutSchedule
	}
	return nil
}

// Validate reports whether spec parses with [Parse]; it
// returns nil or an error matching [ErrInvalid].
func Validate(spec string) error {
	_, err := Parse(spec)
	return err
}
