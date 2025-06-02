package db

import (
	"fmt"
	"strings"
)

type TableBuilder struct {
	schema TableSchema
}

func NewTable(name string) *TableBuilder {
	return &TableBuilder{schema: TableSchema{Name: strings.ToUpper(name)}}
}

func (tb *TableBuilder) AddColumn(name, dataType string) *ColumnBuilder {
	col := ColumnDef{Name: strings.ToUpper(name), Type: dataType}
	tb.schema.Columns = append(tb.schema.Columns, col)
	return &ColumnBuilder{table: tb, column: &tb.schema.Columns[len(tb.schema.Columns)-1]}
}

func (tb *TableBuilder) AddIndex(name string, columns ...string) *TableBuilder {
	upCols := make([]string, len(columns))
	for i, c := range columns {
		upCols[i] = strings.ToUpper(c)
	}
	tb.schema.Indexes = append(tb.schema.Indexes, IndexDef{Name: strings.ToUpper(name), Columns: upCols})
	return tb
}

func (tb *TableBuilder) Build() TableSchema {
	return tb.schema
}

type ColumnBuilder struct {
	table  *TableBuilder
	column *ColumnDef
}

// PrimaryKey marks the column as a primary key.
// For auto-generation, use UUID() or Identity().
func (cb *ColumnBuilder) PrimaryKey() *ColumnBuilder {
	cb.column.IsPrimaryKey = true
	cb.column.IsNotNull = true // PKs are implicitly NOT NULL
	return cb
}

// UUID configures the column as a UUID primary key (RAW(16) with SYS_GUID()).
func (cb *ColumnBuilder) UUID() *ColumnBuilder {
	cb.column.IsPrimaryKey = true
	cb.column.IsNotNull = true
	cb.column.Type = "RAW(16)"
	cb.column.Default = "SYS_GUID()" // Oracle's function to generate GUIDs
	cb.column.IsIdentity = false     // Explicitly not an integer identity
	return cb
}

// Identity configures the column as an auto-incrementing integer primary key (Oracle 12c+).
// Default type is NUMBER(19), can be overridden by setting Type before calling Identity.
func (cb *ColumnBuilder) Identity() *ColumnBuilder {
	cb.column.IsPrimaryKey = true
	cb.column.IsNotNull = true
	cb.column.IsIdentity = true
	if cb.column.Type == "" || cb.column.Type == "RAW(16)" { // Default or if UUID was called before
		cb.column.Type = "NUMBER(19)" // Default integer type for identity
	}
	// The "GENERATED AS IDENTITY" part will be added by buildColumnDefinition
	cb.column.Default = "" // Default is handled by IDENTITY
	return cb
}

func (cb *ColumnBuilder) NotNull() *ColumnBuilder {
	cb.column.IsNotNull = true
	return cb
}

func (cb *ColumnBuilder) Unique() *ColumnBuilder {
	cb.column.IsUnique = true
	return cb
}

func (cb *ColumnBuilder) Size(size int) *ColumnBuilder {
	cb.column.Size = size
	return cb
}

// Default sets a default value for the column.
// Avoid using with Identity() or UUID() as they set their own defaults.
func (cb *ColumnBuilder) Default(value string) *ColumnBuilder {
	if cb.column.IsIdentity || (cb.column.Type == "RAW(16)" && cb.column.Default == "SYS_GUID()") {
		fmt.Printf("%sWarning: Default() called on a column that is already an Identity or UUID. Overwriting default may have unintended consequences.%s\n", ColorYellow, ColorReset)
	}
	cb.column.Default = value
	return cb
}

func (cb *ColumnBuilder) ForeignKey(refTable, refCol string) *ColumnBuilder {
	if cb.column.ForeignKey == nil {
		cb.column.ForeignKey = &ForeignKeyDef{}
	}
	cb.column.ForeignKey.ReferencedTable = strings.ToUpper(refTable)
	cb.column.ForeignKey.ReferencedColumn = strings.ToUpper(refCol)
	return cb
}

func (cb *ColumnBuilder) OnDelete(action string) *ColumnBuilder {
	if cb.column.ForeignKey != nil {
		cb.column.ForeignKey.OnDelete = action
	}
	return cb
}

func (cb *ColumnBuilder) Done() *TableBuilder {
	return cb.table
}
