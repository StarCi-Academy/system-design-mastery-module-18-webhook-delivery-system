// Package config reads runtime configuration from environment variables once at
// bootstrap. POSTGRES_URL / REDIS_URL / PORT mirror the values declared in the
// compose.yaml, so no .env file is needed when running via Docker Compose.
package config

import "os"

// Config holds the process-wide settings shared by emitter and receiver.
type Config struct {
	Port        string
	PostgresURL string
	RedisURL    string
	ReceiverURL string
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Load reads every setting from the environment, falling back to the
// Compose-internal service hostnames when a variable is unset.
func Load() Config {
	return Config{
		Port:        getenv("PORT", "3000"),
		PostgresURL: getenv("POSTGRES_URL", "postgres://app:app@postgres:5432/app"),
		RedisURL:    getenv("REDIS_URL", "redis://redis:6379"),
		ReceiverURL: getenv("RECEIVER_URL", "http://receiver-mock:3000/api/hook"),
	}
}
