package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	jwt.RegisteredClaims
}

type Manager struct {
	secret      []byte
	issuer      string
	subject     string
	ttl         time.Duration
	staticToken string
	expiresAt   *time.Time
}

func NewManager(secret string, issuer string, subject string, ttl time.Duration) (*Manager, error) {
	manager := &Manager{
		secret:  []byte(secret),
		issuer:  issuer,
		subject: subject,
		ttl:     ttl,
	}

	if ttl > 0 {
		return manager, nil
	}

	token, expiresAt, err := manager.generateToken(time.Now().UTC())
	if err != nil {
		return nil, err
	}

	manager.staticToken = token
	manager.expiresAt = expiresAt
	return manager, nil
}

func (m *Manager) Token() (string, *time.Time, error) {
	if m.ttl <= 0 {
		return m.staticToken, m.expiresAt, nil
	}

	return m.generateToken(time.Now().UTC())
}

func (m *Manager) Validate(token string) error {
	parsed, err := jwt.ParseWithClaims(token, &Claims{}, func(candidate *jwt.Token) (any, error) {
		if candidate.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, fmt.Errorf("unexpected signing method: %s", candidate.Method.Alg())
		}

		return m.secret, nil
	})
	if err != nil {
		return err
	}

	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid {
		return errors.New("invalid token")
	}

	if claims.Issuer != m.issuer {
		return fmt.Errorf("unexpected issuer: %s", claims.Issuer)
	}

	if claims.Subject != m.subject {
		return fmt.Errorf("unexpected subject: %s", claims.Subject)
	}

	return nil
}

func (m *Manager) generateToken(now time.Time) (string, *time.Time, error) {
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:  m.issuer,
			Subject: m.subject,
		},
	}

	var expiresAt *time.Time
	if m.ttl > 0 {
		expiry := now.Add(m.ttl)
		claims.IssuedAt = jwt.NewNumericDate(now)
		claims.NotBefore = jwt.NewNumericDate(now)
		claims.ExpiresAt = jwt.NewNumericDate(expiry)
		expiresAt = &expiry
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(m.secret)
	if err != nil {
		return "", nil, err
	}

	return signed, expiresAt, nil
}
