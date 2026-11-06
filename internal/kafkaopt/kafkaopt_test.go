package kafkaopt

import "testing"

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLoad(t *testing.T) {
	c, err := Load(env(map[string]string{"KAFKA_BROKERS": "a:9092, b:9092", "KAFKA_TLS": "true",
		"KAFKA_SASL_MECHANISM": "scram-sha-512", "KAFKA_SASL_USER": "u", "KAFKA_SASL_PASSWORD": "p"}), "svc")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Brokers) != 2 || !c.TLS || c.SASLMechanism != "SCRAM-SHA-512" || c.ClientID != "svc" {
		t.Fatalf("config = %+v", c)
	}
	if n := len(c.Opts()); n != 4 {
		t.Fatalf("opts = %d, want 4", n)
	}
}

func TestLoadErrors(t *testing.T) {
	for name, m := range map[string]map[string]string{
		"no brokers":    {},
		"bad mechanism": {"KAFKA_BROKERS": "a", "KAFKA_SASL_MECHANISM": "GSSAPI"},
		"no creds":      {"KAFKA_BROKERS": "a", "KAFKA_SASL_MECHANISM": "PLAIN"},
	} {
		if _, err := Load(env(m), "svc"); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
