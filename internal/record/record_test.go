package record

import (
	"errors"
	"strings"
	"testing"
)

// valid mirrors the JSON produced by proview-event-collector (event.Record).
const valid = `{"v":1,"event_id":"imp-1:0","type":"impression","impression_id":"imp-1","banner_id":42,"placement":"home_top","event_ts":1794000030000,"received_ts":1794000031000,"duration_ms":0,"visitor_id":"v1","country":"RU","device":"desktop","page_host":"shop.example"}`

func TestDecodeValid(t *testing.T) {
	r, err := Decode([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if r.BannerID != 42 || r.Type != "impression" || r.EventTime().UnixMilli() != 1794000030000 || r.ReceivedTime().Location().String() != "UTC" {
		t.Fatalf("row = %+v", r)
	}
}

func TestDecodeInvalid(t *testing.T) {
	cases := map[string]string{
		"not json":     `{`,
		"version":      strings.Replace(valid, `"v":1`, `"v":2`, 1),
		"no event id":  strings.Replace(valid, `"event_id":"imp-1:0"`, `"event_id":""`, 1),
		"unknown type": strings.Replace(valid, `"impression"`, `"scroll"`, 1),
		"no banner":    strings.Replace(valid, `"banner_id":42`, `"banner_id":0`, 1),
		"no ts":        strings.Replace(valid, `"event_ts":1794000030000`, `"event_ts":0`, 1),
	}
	for name, in := range cases {
		if _, err := Decode([]byte(in)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}
