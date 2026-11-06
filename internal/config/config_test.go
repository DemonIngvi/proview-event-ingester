package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLoad(t *testing.T) {
	c, err := Load(env(map[string]string{"KAFKA_BROKERS": "k:9092", "CLICKHOUSE_ADDRS": "ch1:9000,ch2:9000"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.BatchSize != 20000 || c.FlushInterval != time.Second || len(c.ClickHouse.Addrs) != 2 || c.ClickHouse.DBEngine != "Atomic" {
		t.Fatalf("config = %+v", c)
	}
}

func TestLoadErrors(t *testing.T) {
	_, err := Load(env(map[string]string{"BATCH_SIZE": "0"}))
	for _, want := range []string{"KAFKA_BROKERS", "CLICKHOUSE_ADDRS", "BATCH_SIZE"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error %v does not mention %s", err, want)
		}
	}
}
