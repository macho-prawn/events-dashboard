package auth

import (
	"testing"
	"time"
)

func TestManagerValidate(t *testing.T) {
	manager, err := NewManager("top-secret", "issuer", "subject", time.Hour)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	token, _, err := manager.Token()
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}

	if err := manager.Validate(token); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestManagerRejectsWrongSignature(t *testing.T) {
	manager, err := NewManager("top-secret", "issuer", "subject", 0)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	other, err := NewManager("other-secret", "issuer", "subject", 0)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	token, _, err := other.Token()
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}

	if err := manager.Validate(token); err == nil {
		t.Fatal("Validate() expected error, got nil")
	}
}

func TestManagerRejectsExpiredToken(t *testing.T) {
	manager, err := NewManager("top-secret", "issuer", "subject", time.Nanosecond)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	token, _, err := manager.Token()
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}

	time.Sleep(2 * time.Millisecond)

	if err := manager.Validate(token); err == nil {
		t.Fatal("Validate() expected error, got nil")
	}
}

func TestManagerPersistentTokenWithoutTTL(t *testing.T) {
	managerA, err := NewManager("top-secret", "issuer", "subject", 0)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	time.Sleep(2 * time.Millisecond)

	managerB, err := NewManager("top-secret", "issuer", "subject", 0)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	tokenA, expiresA, err := managerA.Token()
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}

	tokenB, expiresB, err := managerB.Token()
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}

	if tokenA != tokenB {
		t.Fatalf("persistent token mismatch: %q != %q", tokenA, tokenB)
	}

	if expiresA != nil || expiresB != nil {
		t.Fatalf("expected nonexpiring tokens, got expiresA=%v expiresB=%v", expiresA, expiresB)
	}
}
