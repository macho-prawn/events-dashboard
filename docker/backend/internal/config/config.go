package config

import (
	"fmt"
	"net"
	"os"

	"github.com/macho_prawn/events-dashboard/internal/models"
)

type Config struct {
	PublicServerAddr       string
	DatabaseURL            string
}

func Load() (Config, error) {
	cfg := Config{
		PublicServerAddr:       buildAddr(getFirstEnv("HOST", "APP_HOST", "0.0.0.0"), getFirstEnv("PORT", "APP_PORT", "8081")),
		DatabaseURL:            buildDatabaseURL(),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func buildDatabaseURL() string {
	if databaseURL := os.Getenv("DATABASE_URL"); databaseURL != "" {
		return databaseURL
	}

	host := getEnv("DB_HOST", "db")
	port := getEnv("DB_PORT", "5432")
	name := os.Getenv("DB_NAME")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	sslMode := getEnv("DB_SSLMODE", "disable")

	if host == "" || port == "" || name == "" || user == "" || password == "" {
		return ""
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, password, host, port, name, sslMode)
}

func buildAddr(host string, port string) string {
	return net.JoinHostPort(host, port)
}

func getEnv(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func getFirstEnv(primary string, secondary string, fallback string) string {
	if value := os.Getenv(primary); value != "" {
		return value
	}

	if value := os.Getenv(secondary); value != "" {
		return value
	}

	return fallback
}

func (Config) DefaultAPIKeyAccess() models.APIKeyAccess {
	return models.APIKeyAccess{
		AccessSigningSecret:    "change-api-key-access-secret",
		AccessIssuer:           "events-dashboard",
		AccessSubject:          "events-api-key-access",
		IngestionSigningSecret: "change-me",
		IngestionIssuer:        "events-dashboard",
		IngestionSubject:       "events-ingestion",
		IngestionTTLSeconds:    3600,
	}
}
