package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type TableColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type TableSchema []TableColumn

func (s TableSchema) Value() (driver.Value, error) {
	if s == nil {
		return []byte("[]"), nil
	}

	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *TableSchema) Scan(value any) error {
	switch candidate := value.(type) {
	case nil:
		*s = TableSchema{}
		return nil
	case []byte:
		return json.Unmarshal(candidate, s)
	case string:
		return json.Unmarshal([]byte(candidate), s)
	default:
		return fmt.Errorf("unsupported table schema type %T", value)
	}
}
