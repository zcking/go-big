package gobig

import (
	"os"
	"strconv"
)

// GetEnvInt parses an integer value from environment variable key
func GetEnvInt(key string, defaultValue int) int {
	s := os.Getenv(key)
	if s == "" {
		return defaultValue
	}

	i, _ := strconv.Atoi(s)
	return i
}
