package config

import (
	"log"
	"os"
)

type Config struct {
	DBUrl        string
	RedisAddr    string
	KafkaBrokers []string
	JwtSecret    string
}

func LoadConfig() *Config {
	cfg := &Config{
		DBUrl:        getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/portfolio_db?sslmode=disable"),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers: []string{getEnv("KAFKA_BROKER", "localhost:9092")},
		JwtSecret:    getEnv("JWT_SECRET", "secret"),
	}
	log.Printf("Loaded config: DB=%s Redis=%s Kafka=%v", cfg.DBUrl, cfg.RedisAddr, cfg.KafkaBrokers)
	return cfg
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
