package common

import (
	"fmt"
	"log/slog"
	"os"
	"path"
	"strconv"
	"strings"
)

// Settings:
// port
// db connection string

const DEFAULT_PORT = 48008
const DEFAULT_DSN = ":memory:"

var log_level_mapping = map[string]slog.Level{
	"DEBUG": slog.LevelDebug,
	"INFO":  slog.LevelInfo,
	"WARN":  slog.LevelWarn,
}

type Config struct {
	Port        int        `json:"port"`
	Dsn         string     `json:"dsn"`
	StoragePath string     `json:"storage_path"`
	LogLevel    slog.Level `json:"log_level"`
	APIPrefix   string     `json:"api_prefix"`
}

func defaultStoragePath() string {
	home_dir := os.Getenv("HOME")
	if len(home_dir) == 0 {
		slog.Error("Error: environment variable HOME not set\n")
		os.Exit(1)
	}
	data_home := os.Getenv("XDG_DATA_HOME")
	if len(data_home) == 0 {
		data_home = path.Join(home_dir, ".local/share")
	}
	return path.Join(data_home, "govalent", "data")
}

func newDefaultConfig() Config {
	return Config{
		Port:        DEFAULT_PORT,
		Dsn:         DEFAULT_DSN,
		StoragePath: defaultStoragePath(),
		LogLevel:    slog.LevelInfo,
		APIPrefix:   "",
	}
}

// TODO
func NewConfigFromEnv() Config {
	c := newDefaultConfig()
	dsn := os.Getenv("GOVALENT_DSN")
	if len(dsn) > 0 {
		c.Dsn = dsn
	}
	port := os.Getenv("GOVALENT_PORT")
	if len(port) > 0 {
		portNum, err := strconv.Atoi(port)
		if err != nil {
			slog.Error(fmt.Sprint("Error parsing port number: ", err.Error()))
			os.Exit(1)
		}
		c.Port = portNum
	}

	data_dir := os.Getenv("GOVALENT_DATA_DIR")
	if len(data_dir) > 0 {
		c.StoragePath = data_dir
	}

	log_level := os.Getenv("GOVALENT_LOG_LEVEL")
	if len(log_level) > 0 {
		level, ok := log_level_mapping[strings.ToUpper(log_level)]
		if !ok {
			slog.Error(fmt.Sprint("Invalid log level ", log_level))
			os.Exit(1)
		}
		c.LogLevel = level
	}
	api_prefix := os.Getenv("GOVALENT_API_PREFIX")
	if len(api_prefix) > 0 {
		c.APIPrefix = api_prefix
	}
	return c
}
