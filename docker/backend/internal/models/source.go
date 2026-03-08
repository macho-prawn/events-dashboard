package models

import "time"

type Source struct {
	ID             uint        `gorm:"primaryKey"`
	Source         string      `gorm:"not null;index:idx_source_identity,unique;index:idx_source_owner"`
	Company        string      `gorm:"not null;index:idx_source_identity,unique;index:idx_source_owner"`
	City           string      `gorm:"not null;index:idx_source_identity,unique"`
	State          string      `gorm:"not null;index:idx_source_identity,unique"`
	Country        string      `gorm:"not null;index:idx_source_identity,unique"`
	ChildTableName string      `gorm:"column:child_table_name;not null"`
	TableSchema    TableSchema `gorm:"column:table_schema;type:jsonb;not null"`
	CreatedAt      time.Time   `gorm:"not null"`
	UpdatedAt      time.Time   `gorm:"not null"`
}

func (Source) TableName() string {
	return "sources"
}
