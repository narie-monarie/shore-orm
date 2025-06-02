package db

const (
	ColorReset  = "\033[0m"
	ColorCyan   = "\033[36m"
	ColorYellow = "\033[33m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
)

type ForeignKeyDef struct {
	ReferencedTable  string
	ReferencedColumn string
	OnDelete         string
	OnUpdate         string
}

type TableSchema struct {
	Name    string
	Columns []ColumnDef
	Indexes []IndexDef
}

func (ts TableSchema) Q() QTable {
	return QTable{Name: ts.Name}
}

func (ts TableSchema) Match(columnName string) QColumn {
	qTable := ts.Q()
	return NewQColumn(&qTable, columnName)
}

func (ts TableSchema) All() QSelectable {
	qTable := ts.Q()
	return QAllColumns{Table: &qTable}
}

type ColumnDef struct {
	Name         string
	Type         string
	IsPrimaryKey bool
	IsIdentity   bool
	IsNotNull    bool
	IsUnique     bool
	Default      string
	Size         int
	ForeignKey   *ForeignKeyDef
}

type IndexDef struct {
	Name     string
	Columns  []string
	IsUnique bool
}
