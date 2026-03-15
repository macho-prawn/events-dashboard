package models

import "time"

type Project struct {
	ID                    uint       `gorm:"primaryKey"`
	ProjectName           string     `gorm:"column:project_name;size:10;not null;uniqueIndex"`
	IngestionJWT          string     `gorm:"column:ingestion_jwt;type:text;not null"`
	IngestionJWTExpiresAt *time.Time `gorm:"column:ingestion_jwt_expires_at;type:timestamptz"`
	CreatedAt             time.Time  `gorm:"not null"`
	UpdatedAt             time.Time  `gorm:"not null"`
}

func (Project) TableName() string {
	return "projects"
}
