package models

import "time"

type APIKeyAccess struct {
	ID                     uint      `gorm:"primaryKey"`
	AccessSigningSecret    string    `gorm:"column:signing_secret;not null"`
	AccessIssuer           string    `gorm:"column:issuer;not null"`
	AccessSubject          string    `gorm:"column:subject;not null"`
	IngestionSigningSecret string    `gorm:"column:ingestion_signing_secret"`
	IngestionIssuer        string    `gorm:"column:ingestion_issuer"`
	IngestionSubject       string    `gorm:"column:ingestion_subject"`
	IngestionTTLSeconds    int       `gorm:"column:ingestion_ttl_seconds"`
	CreatedAt              time.Time `gorm:"not null"`
	UpdatedAt              time.Time `gorm:"not null"`
}

func (APIKeyAccess) TableName() string {
	return "api_key_access"
}
