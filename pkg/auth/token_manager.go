// Package auth provides authentication utilities including Firebase token management
package auth

import (
	"context"
	"sync"
	"time"

	firebase "firebase.google.com/go/v4"
	firebaseAuth "firebase.google.com/go/v4/auth"
	"google.golang.org/api/option"
)

// TokenManager handles Firebase custom token generation and caching.
// It automatically refreshes tokens before expiration and is safe for concurrent use.
type TokenManager interface {
	GetToken() (string, error)
	Refresh() (string, error)
}

type tokenManager struct {
	auth      *firebaseAuth.Client
	token     string
	expiresAt time.Time
	mu        sync.RWMutex
	serviceID string
}

// NewTokenManager creates a TokenManager for service-to-service authentication.
// The serviceID parameter is used to identify your service in Firebase logs.
func NewTokenManager(serviceID string, credentialsFile ...string) (TokenManager, error) {
	var opts []option.ClientOption

	if len(credentialsFile) > 0 && credentialsFile[0] != "" {
		opts = append(opts, option.WithCredentialsFile(credentialsFile[0]))
	}

	app, err := firebase.NewApp(context.Background(), nil, opts...)
	if err != nil {
		return nil, err
	}

	auth, err := app.Auth(context.Background())
	if err != nil {
		return nil, err
	}

	return &tokenManager{
		auth:      auth,
		serviceID: serviceID,
	}, nil
}

// GetToken returns a valid Firebase custom token.
func (tm *tokenManager) GetToken() (string, error) {
	tm.mu.RLock()
	if tm.token != "" && time.Until(tm.expiresAt) > 5*time.Minute {
		token := tm.token
		tm.mu.RUnlock()
		return token, nil
	}
	tm.mu.RUnlock()
	return tm.Refresh()
}

// refresh generates a new Firebase custom token.
func (tm *tokenManager) Refresh() (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	token, err := tm.auth.CustomToken(context.Background(), tm.serviceID)
	if err != nil {
		return "", err
	}

	tm.token = token
	tm.expiresAt = time.Now().Add(55 * time.Minute)
	return token, nil
}
