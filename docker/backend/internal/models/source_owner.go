package models

import "time"

type SourceOwner struct {
	ID             uint        `gorm:"primaryKey"`
	Source         string      `gorm:"not null;uniqueIndex:idx_source_owner_identity"`
	Company        string      `gorm:"not null;uniqueIndex:idx_source_owner_identity"`
	WebsiteDomain  string      `gorm:"column:website_domain"`
	ChildTableName string      `gorm:"column:child_table_name;not null"`
	TableSchema    TableSchema `gorm:"column:table_schema;type:jsonb;not null"`
	CreatedAt      time.Time   `gorm:"not null"`
	UpdatedAt      time.Time   `gorm:"not null"`
}

func (SourceOwner) TableName() string {
	return "source_owners"
}
