package storage

import "context"

type Row interface {
	Type() string
	ID() string
	Label() string
	ParentID() string
	Columns() map[string]interface{}
}

type RowStorer interface {
	GetRowByID(ctx context.Context, rowType, rowID string) (Row, error)
	GetRow(ctx context.Context, rowType, rowLabel string) (Row, error)
	CreateRow(ctx context.Context, rowType, rowLabel string) (Row, error)
	CreateChild(ctx context.Context, rowType, rowLabel, parentType, parentID string, columns map[string]interface{}) (Row, error)
	GetChild(ctx context.Context, childLabel, parentID string) (Row, error)
	ListRows(ctx context.Context, rowType, labelFilter, parentIDFilter string) ([]Row, error)
	UpdateRow(ctx context.Context, rowType, rowID, newLabel string) (Row, error)
	UpdateChild(ctx context.Context, childType, childID, newChildLabel, parentType, newParentID string) (Row, error)
	UpdateColumn(ctx context.Context, rowType, rowID, columnName string, columnValue interface{}) error
	UpdateColumns(ctx context.Context, rowType, rowID string, columns map[string]interface{}) error
	DeleteRow(ctx context.Context, rowType, childType, rowID string) error
}
