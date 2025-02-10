package logger

import (
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/pkg/errors"
)

type Config struct {
	Level  string `envconfig:"level" json:"level"`
	Format string `envconfig:"format" json:"format"`
	Output string `envconfig:"output" json:"output"`
}

var (
	DefaultLevel  slog.Level = slog.LevelInfo
	DefaultOutput io.Writer  = os.Stdout
)

func New(cfg *Config, ow io.Writer) (*slog.Logger, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	var output io.Writer
	if ow != nil {
		output = ow
	} else {
		output = cfg.SlogOutput()
	}

	log := slog.New(cfg.SlogHandler(output))
	log.Info("logger initialized", "level", cfg.Level, "format", cfg.Format, "output", cfg.Output)
	return log, nil
}

func Default() *slog.Logger {
	handler := slog.NewJSONHandler(DefaultOutput, &slog.HandlerOptions{Level: DefaultLevel})
	return slog.New(handler)
}

func (c *Config) SlogLevel() slog.Level {
	switch c.Level {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func (c *Config) SlogOutput() io.Writer {
	switch c.Output {
	case "stderr":
		return os.Stderr
	default:
		return os.Stdout
	}
}

func (c *Config) SlogHandler(ow io.Writer) slog.Handler {
	switch c.Format {
	case "json":
		return slog.NewJSONHandler(ow, &slog.HandlerOptions{Level: c.SlogLevel()})
	default:
		return slog.NewTextHandler(ow, &slog.HandlerOptions{Level: c.SlogLevel()})
	}
}

func (c *Config) Validate() error {
	if !slices.Contains([]string{"DEBUG", "INFO", "WARN", "ERROR"}, strings.ToUpper(c.Level)) {
		return errors.Errorf("invalid log level: %s", c.Level)
	}

	if !slices.Contains([]string{"text", "json"}, c.Format) {
		return errors.Errorf("invalid log format: %s", c.Format)
	}

	return nil
}
