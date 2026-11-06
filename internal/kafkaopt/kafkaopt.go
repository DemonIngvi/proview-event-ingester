// Package kafkaopt builds franz-go client options (brokers, TLS, SASL) from
// environment configuration.
package kafkaopt

import (
	"crypto/tls"
	"errors"
	"strings"

	"github.com/DemonIngvi/proview-event-ingester/internal/platform"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config describes how to reach the Kafka cluster.
type Config struct {
	Brokers       []string
	ClientID      string
	TLS           bool
	SASLMechanism string // "", PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUser      string
	SASLPassword  string
}

// Load reads KAFKA_* variables.
func Load(lookup func(string) (string, bool), clientID string) (Config, error) {
	c := Config{
		Brokers:       platform.EnvList(lookup, "KAFKA_BROKERS", nil),
		ClientID:      platform.Env(lookup, "KAFKA_CLIENT_ID", clientID),
		TLS:           platform.Env(lookup, "KAFKA_TLS", "false") == "true",
		SASLMechanism: strings.ToUpper(platform.Env(lookup, "KAFKA_SASL_MECHANISM", "")),
		SASLUser:      platform.Env(lookup, "KAFKA_SASL_USER", ""),
		SASLPassword:  platform.Env(lookup, "KAFKA_SASL_PASSWORD", ""),
	}
	if len(c.Brokers) == 0 {
		return c, errors.New("KAFKA_BROKERS is required")
	}
	switch c.SASLMechanism {
	case "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		return c, errors.New("KAFKA_SASL_MECHANISM must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512")
	}
	if c.SASLMechanism != "" && (c.SASLUser == "" || c.SASLPassword == "") {
		return c, errors.New("KAFKA_SASL_USER and KAFKA_SASL_PASSWORD are required with SASL")
	}
	return c, nil
}

// Opts returns the connection-level client options.
func (c Config) Opts() []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.ClientID)}
	if c.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	switch c.SASLMechanism {
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: c.SASLUser, Pass: c.SASLPassword}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUser, Pass: c.SASLPassword}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUser, Pass: c.SASLPassword}.AsSha512Mechanism()))
	}
	return opts
}
