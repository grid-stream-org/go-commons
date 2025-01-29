package auth

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type mockTokenManager struct {
	mu         sync.RWMutex
	token      string
	expiresAt  time.Time
	tokenCount int
}

func (m *mockTokenManager) GetToken() (string, error) {
	m.mu.RLock()
	if m.token != "" && time.Until(m.expiresAt) > 5*time.Minute {
		token := m.token
		m.mu.RUnlock()
		return token, nil
	}
	m.mu.RUnlock()
	return m.Refresh()
}

func (m *mockTokenManager) Refresh() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokenCount++
	m.token = fmt.Sprintf("mock-token-%d", m.tokenCount)
	m.expiresAt = time.Now().Add(55 * time.Minute)
	return m.token, nil
}

func TestTokenManager(t *testing.T) {
	tm := &mockTokenManager{}

	// Get initial token
	token1, err := tm.GetToken()
	if err != nil {
		t.Fatalf("Failed to get initial token: %v", err)
	}
	if token1 == "" {
		t.Fatal("Expected non-empty token")
	}

	// Get token again immediately - should return cached token
	token2, err := tm.GetToken()
	if err != nil {
		t.Fatalf("Failed to get second token: %v", err)
	}
	if token2 != token1 {
		t.Error("Expected cached token to be returned")
	}
	if tm.tokenCount != 1 {
		t.Errorf("Expected 1 token generation, got %d", tm.tokenCount)
	}

	// Force token to be near expiry
	tm.expiresAt = time.Now().Add(4 * time.Minute)

	// Get token after expiry - should get new token
	token3, err := tm.GetToken()
	if err != nil {
		t.Fatalf("Failed to get third token: %v", err)
	}
	if token3 == token1 {
		t.Error("Expected new token after expiry")
	}
	if tm.tokenCount != 2 {
		t.Errorf("Expected 2 token generations, got %d", tm.tokenCount)
	}
}

func TestTokenManagerConcurrent(t *testing.T) {
	tm := &mockTokenManager{}

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			token, err := tm.GetToken()
			if err != nil {
				t.Errorf("Failed to get token: %v", err)
			}
			if token == "" {
				t.Error("Got empty token")
			}
		}()
	}

	wg.Wait()

	if tm.tokenCount != 1 {
		t.Errorf("Expected 1 token generation, got %d", tm.tokenCount)
	}
}
