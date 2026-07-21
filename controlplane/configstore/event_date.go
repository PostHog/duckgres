package configstore

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// eventDateLayout is the wire and display format of EventDate values.
const eventDateLayout = "2006-01-02"

// EventDate is a calendar date (a Postgres DATE — no time-of-day, no zone)
// serialized as "YYYY-MM-DD" in JSON. It backs OrgTeam.EarliestEventDate.
type EventDate struct {
	time.Time
}

// ParseEventDate parses a strict "YYYY-MM-DD" string into an EventDate.
func ParseEventDate(s string) (EventDate, error) {
	t, err := time.ParseInLocation(eventDateLayout, s, time.UTC)
	if err != nil {
		return EventDate{}, errors.New("must be a YYYY-MM-DD date")
	}
	return EventDate{Time: t}, nil
}

func (d EventDate) String() string { return d.Format(eventDateLayout) }

func (d EventDate) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

func (d *EventDate) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := ParseEventDate(s)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

// Value stores the date as a midnight-UTC time.Time for the DATE column.
func (d EventDate) Value() (driver.Value, error) { return d.Time, nil }

// Scan reads a DATE column (time.Time from the driver; string/[]byte
// defensively), normalizing to midnight UTC so round-trips compare equal.
func (d *EventDate) Scan(v interface{}) error {
	switch t := v.(type) {
	case nil:
		*d = EventDate{}
		return nil
	case time.Time:
		*d = EventDate{Time: time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)}
		return nil
	case string:
		parsed, err := ParseEventDate(t)
		if err != nil {
			return fmt.Errorf("scan EventDate from %q: %w", t, err)
		}
		*d = parsed
		return nil
	case []byte:
		return d.Scan(string(t))
	default:
		return fmt.Errorf("scan EventDate: unsupported type %T", v)
	}
}

// GormDataType pins the column type so AutoMigrate (and the goose/GORM
// metadata-match test) agree with migration 000026's DATE column.
func (EventDate) GormDataType() string { return "date" }
