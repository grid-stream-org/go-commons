package logger

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type LoggerTestSuite struct {
	suite.Suite
}

func (s *LoggerTestSuite) TestNew() {
	testCases := []struct {
		name        string
		cfg         *Config
		writer      io.Writer
		expectError bool
		validate    func(*testing.T, *slog.Logger, error, *bytes.Buffer)
	}{
		{
			name: "Valid JSON logger",
			cfg: &Config{
				Level:  "INFO",
				Format: "json",
				Output: "stdout",
			},
			writer: nil,
			validate: func(t *testing.T, logger *slog.Logger, err error, _ *bytes.Buffer) {
				s.NoError(err)
				s.NotNil(logger)
			},
		},
		{
			name: "Valid text logger with custom writer",
			cfg: &Config{
				Level:  "DEBUG",
				Format: "text",
				Output: "stdout",
			},
			writer: new(bytes.Buffer),
			validate: func(t *testing.T, logger *slog.Logger, err error, buf *bytes.Buffer) {
				s.NoError(err)
				s.NotNil(logger)
				logger.Info("test message")
				s.Contains(buf.String(), "test message")
				s.Contains(buf.String(), "level=INFO")
			},
		},
		{
			name: "Invalid level",
			cfg: &Config{
				Level:  "INVALID",
				Format: "json",
				Output: "stdout",
			},
			expectError: true,
			validate: func(t *testing.T, logger *slog.Logger, err error, _ *bytes.Buffer) {
				s.Error(err)
				s.Nil(logger)
				s.Contains(err.Error(), "invalid log level")
			},
		},
		{
			name: "Invalid format",
			cfg: &Config{
				Level:  "INFO",
				Format: "invalid",
				Output: "stdout",
			},
			expectError: true,
			validate: func(t *testing.T, logger *slog.Logger, err error, _ *bytes.Buffer) {
				s.Error(err)
				s.Nil(logger)
				s.Contains(err.Error(), "invalid log format")
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			var buf *bytes.Buffer
			var writer io.Writer = tc.writer
			if writer == nil {
				buf = new(bytes.Buffer)
				writer = buf
			} else {
				buf = writer.(*bytes.Buffer)
			}

			logger, err := New(tc.cfg, writer)
			tc.validate(s.T(), logger, err, buf)
		})
	}
}

func (s *LoggerTestSuite) TestDefault() {
	logger := Default()
	s.NotNil(logger)

	// Test that it uses default settings
	s.Equal(DefaultLevel, slog.LevelInfo)
	s.Equal(DefaultOutput, os.Stdout)
}

func (s *LoggerTestSuite) TestConfigSlogLevel() {
	testCases := []struct {
		level    string
		expected slog.Level
	}{
		{"DEBUG", slog.LevelDebug},
		{"WARN", slog.LevelWarn},
		{"ERROR", slog.LevelError},
		{"INFO", slog.LevelInfo},
		{"invalid", slog.LevelInfo}, // default case
	}

	for _, tc := range testCases {
		s.Run(tc.level, func() {
			cfg := &Config{Level: tc.level}
			s.Equal(tc.expected, cfg.SlogLevel())
		})
	}
}

func (s *LoggerTestSuite) TestConfigSlogOutput() {
	testCases := []struct {
		output   string
		expected io.Writer
	}{
		{"stderr", os.Stderr},
		{"stdout", os.Stdout},
		{"invalid", os.Stdout}, // default case
	}

	for _, tc := range testCases {
		s.Run(tc.output, func() {
			cfg := &Config{Output: tc.output}
			s.Equal(tc.expected, cfg.SlogOutput())
		})
	}
}

func (s *LoggerTestSuite) TestConfigSlogHandler() {
	testCases := []struct {
		name       string
		format     string
		level      string
		validateFn func(*bytes.Buffer, slog.Handler)
	}{
		{
			name:   "JSON Handler",
			format: "json",
			level:  "INFO",
			validateFn: func(buf *bytes.Buffer, h slog.Handler) {
				logger := slog.New(h)
				logger.Info("test message", "key", "value")

				// Read the JSON output
				var logEntry map[string]interface{}
				err := json.Unmarshal(buf.Bytes(), &logEntry)
				s.NoError(err)
				s.Equal("test message", logEntry["msg"])
				s.Equal("value", logEntry["key"])
			},
		},
		{
			name:   "Text Handler",
			format: "text",
			level:  "DEBUG",
			validateFn: func(buf *bytes.Buffer, h slog.Handler) {
				logger := slog.New(h)
				logger.Info("test message", "key", "value")

				output := buf.String()
				s.Contains(output, "test message")
				s.Contains(output, "key=value")
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			buf := new(bytes.Buffer)
			cfg := &Config{
				Format: tc.format,
				Level:  tc.level,
			}
			handler := cfg.SlogHandler(buf)
			s.NotNil(handler)
			tc.validateFn(buf, handler)
		})
	}
}

func (s *LoggerTestSuite) TestConfigValidate() {
	testCases := []struct {
		name        string
		cfg         *Config
		expectError bool
	}{
		{
			name: "Valid config",
			cfg: &Config{
				Level:  "INFO",
				Format: "json",
			},
			expectError: false,
		},
		{
			name: "Invalid level",
			cfg: &Config{
				Level:  "INVALID",
				Format: "json",
			},
			expectError: true,
		},
		{
			name: "Invalid format",
			cfg: &Config{
				Level:  "INFO",
				Format: "invalid",
			},
			expectError: true,
		},
		{
			name: "Case insensitive level",
			cfg: &Config{
				Level:  "info",
				Format: "json",
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			err := tc.cfg.Validate()
			if tc.expectError {
				s.Error(err)
			} else {
				s.NoError(err)
			}
		})
	}
}

func TestLoggerSuite(t *testing.T) {
	suite.Run(t, new(LoggerTestSuite))
}
