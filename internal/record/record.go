// Package record decodes and validates event messages from Kafka topic
// proview.events.v1 (contract owned by proview-event-collector).
package record

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Row is one event as stored in ClickHouse proview.events_raw.
type Row struct {
	V            int    `json:"v"`
	EventID      string `json:"event_id"`
	Type         string `json:"type"`
	ImpressionID string `json:"impression_id"`
	BannerID     uint64 `json:"banner_id"`
	Placement    string `json:"placement"`
	EventTS      int64  `json:"event_ts"`
	ReceivedTS   int64  `json:"received_ts"`
	DurationMS   uint32 `json:"duration_ms"`
	VisitorID    string `json:"visitor_id"`
	Country      string `json:"country"`
	Device       string `json:"device"`
	PageHost     string `json:"page_host"`
}

var knownTypes = map[string]bool{"impression": true, "viewable": true, "hover": true, "click": true, "active_time": true}

// ErrInvalid wraps every decoding/validation failure; such messages go to the DLQ.
var ErrInvalid = errors.New("invalid record")

// Decode parses and validates a Kafka message value.
func Decode(b []byte) (Row, error) {
	var r Row
	if err := json.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("%w: %v", ErrInvalid, err) //nolint:errorlint // single sentinel wrap is intended
	}
	switch {
	case r.V != 1:
		return r, fmt.Errorf("%w: unsupported schema version %d", ErrInvalid, r.V)
	case r.EventID == "" || r.ImpressionID == "":
		return r, fmt.Errorf("%w: missing ids", ErrInvalid)
	case !knownTypes[r.Type]:
		return r, fmt.Errorf("%w: unknown type %q", ErrInvalid, r.Type)
	case r.BannerID == 0 || r.Placement == "":
		return r, fmt.Errorf("%w: missing banner or placement", ErrInvalid)
	case r.EventTS <= 0 || r.ReceivedTS <= 0:
		return r, fmt.Errorf("%w: missing timestamps", ErrInvalid)
	}
	return r, nil
}

// EventTime returns the event timestamp in UTC.
func (r Row) EventTime() time.Time { return time.UnixMilli(r.EventTS).UTC() }

// ReceivedTime returns the server receive timestamp in UTC.
func (r Row) ReceivedTime() time.Time { return time.UnixMilli(r.ReceivedTS).UTC() }
