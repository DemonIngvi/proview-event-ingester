package chstore

import "crypto/tls"

func tlsConfig() *tls.Config { return &tls.Config{MinVersion: tls.VersionTLS12} }
