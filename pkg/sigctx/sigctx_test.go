package sigctx

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type SignalContextTestSuite struct {
	suite.Suite
}

func (s *SignalContextTestSuite) TestNew() {
	// Test basic context creation
	ctx, cancel := New(context.Background())
	defer cancel()

	s.NotNil(ctx, "Context should not be nil")
	s.NotNil(cancel, "Cancel function should not be nil")
}

func (s *SignalContextTestSuite) TestSignalError() {
	// Test SignalError string representation
	sigErr := &SignalError{Signal: syscall.SIGTERM}
	s.Equal("received signal: terminated", sigErr.Error())
}

func (s *SignalContextTestSuite) TestSignalErrorSigNum() {
	testCases := []struct {
		name     string
		signal   os.Signal
		expected int
	}{
		{
			name:     "SIGTERM",
			signal:   syscall.SIGTERM,
			expected: int(syscall.SIGTERM),
		},
		{
			name:     "SIGINT",
			signal:   syscall.SIGINT,
			expected: int(syscall.SIGINT),
		},
		{
			name:     "Non-syscall signal",
			signal:   mockSignal{},
			expected: 1,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			sigErr := &SignalError{Signal: tc.signal}
			s.Equal(tc.expected, sigErr.SigNum())
		})
	}
}

func (s *SignalContextTestSuite) TestContextCancellation() {
	ctx, cancel := New(context.Background())
	defer cancel()

	// Test normal cancellation
	cancel()

	// Wait for context to be cancelled
	select {
	case <-ctx.Done():
		s.NotNil(ctx.Err(), "Context should have an error after cancellation")
	case <-time.After(time.Second):
		s.Fail("Context should have been cancelled")
	}
}

func (s *SignalContextTestSuite) TestSignalHandling() {
	ctx, cancel := New(context.Background())
	defer cancel()

	// Get the concrete type to access the signal error
	sigCtx, ok := ctx.(*signalContext)
	s.True(ok, "Context should be of type *signalContext")

	// Simulate a signal
	proc, err := os.FindProcess(os.Getpid())
	s.NoError(err, "Should be able to find current process")

	err = proc.Signal(syscall.SIGTERM)
	s.NoError(err, "Should be able to send signal")

	// Wait for signal to be processed
	time.Sleep(100 * time.Millisecond)

	// Verify signal was captured
	s.NotNil(sigCtx.Err(), "Context should have an error")
	sigErr, ok := sigCtx.Err().(*SignalError)
	s.True(ok, "Error should be of type *SignalError")
	s.Equal(syscall.SIGTERM, sigErr.Signal)
}

// mockSignal implements os.Signal interface for testing
type mockSignal struct{}

func (m mockSignal) String() string { return "mock" }
func (m mockSignal) Signal()        {}

func TestSignalContextSuite(t *testing.T) {
	suite.Run(t, new(SignalContextTestSuite))
}
