package sigctx

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/grid-stream-org/batcher/pkg/logger"
)

type SignalError struct {
	Signal os.Signal
}

func (e *SignalError) Error() string {
	return "received signal: " + e.Signal.String()
}

func (e *SignalError) SigNum() int {
	if sig, ok := e.Signal.(syscall.Signal); ok {
		return int(sig)
	}
	return 1
}

type signalContext struct {
	context.Context
	mu     sync.Mutex
	sigErr *SignalError
}

func (s *signalContext) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sigErr != nil {
		return s.sigErr
	}
	return s.Context.Err()
}

func New(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	sigCtx := &signalContext{
		Context: ctx,
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Default().Info("shutdown signal received", "signal", sig.String())
		sigCtx.mu.Lock()
		sigCtx.sigErr = &SignalError{Signal: sig}
		sigCtx.mu.Unlock()
		cancel()
		signal.Stop(sigChan)
	}()
	return sigCtx, cancel
}
