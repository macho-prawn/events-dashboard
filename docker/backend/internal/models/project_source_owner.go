package models

import "time"

type ProjectSourceOwner struct {
	ID            uint      `gorm:"primaryKey"`
	ProjectID     uint      `gorm:"column:project_id;not null;uniqueIndex:idx_project_source_owner"`
	SourceOwnerID uint      `gorm:"column:source_owner_id;not null;uniqueIndex:idx_project_source_owner"`
	CreatedAt     time.Time `gorm:"not null"`
	UpdatedAt     time.Time `gorm:"not null"`
}

func (ProjectSourceOwner) TableName() string {
	return "project_source_owners"
}
