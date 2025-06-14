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

func (cb *ColumnBuilder) PrimaryKey() *ColumnBuilder {
	cb.column.IsPrimaryKey = true
	cb.column.IsNotNull = true
	return cb
}

func (cb *ColumnBuilder) UUID() *ColumnBuilder {
	cb.column.IsPrimaryKey = true
	cb.column.IsNotNull = true
	cb.column.Type = "RAW(16)"
	cb.column.Default = "SYS_GUID()" // Oracle's Function To Generate GUIDs
	cb.column.IsIdentity = false
	return cb
}

func (cb *ColumnBuilder) Identity() *ColumnBuilder {
	cb.column.IsPrimaryKey = true
	cb.column.IsNotNull = true
	cb.column.IsIdentity = true // Set the flag
	if cb.column.Type == "" || cb.column.Type == "RAW(16)" {
		cb.column.Type = "NUMBER(19)"
	}
	cb.column.Default = "" // Default is handled by "GENERATED AS IDENTITY"
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
