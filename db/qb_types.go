package db

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type QContext struct {
	params           []any
	placeholderIndex int
	dialect          string
}

func newQContext(dialect string) *QContext {
	return &QContext{dialect: dialect}
}

func (qc *QContext) nextPlaceholder() string {
	qc.placeholderIndex++
	if qc.dialect == "oracle" {
		return fmt.Sprintf(":%d", qc.placeholderIndex)
	}
	return "?"
}

func (qc *QContext) addParam(value any) string {
	if u, ok := value.(uuid.UUID); ok && qc.dialect == "oracle" {
		valueBytes, err := u.MarshalBinary()
		if err != nil {
			panic(fmt.Sprintf("failed to marshal UUID to binary for query parameter: %v", err))
		}
		qc.params = append(qc.params, valueBytes)
		return qc.nextPlaceholder()
	}
	qc.params = append(qc.params, value)
	return qc.nextPlaceholder()
}

type QTable struct {
	Name  string // Actual DB table name
	Alias string
}

func (qt QTable) GetNameOrAlias() string {
	if qt.Alias != "" {
		return qt.Alias
	}
	return qt.Name
}

func (qt QTable) GetQualifiedName() string {
	return qt.GetNameOrAlias()
}

type QColumn struct {
	Name    string // DB column name
	Table   *QTable
	sqlName string // e.g., "TABLE_NAME.COLUMN_NAME"
}

func NewQColumn(table *QTable, columnName string) QColumn {
	colNameUpper := strings.ToUpper(columnName)
	return QColumn{Name: colNameUpper, Table: table, sqlName: fmt.Sprintf("%s.%s", table.Name, colNameUpper)}
}

func (qc QColumn) As(alias string) QSelectable {
	return QAliasedExpression{Expression: qc, AliasName: strings.ToUpper(alias)}
}
func (qc QColumn) Asc() QOrderTerm  { return QOrderTerm{Column: qc, Direction: "ASC"} }
func (qc QColumn) Desc() QOrderTerm { return QOrderTerm{Column: qc, Direction: "DESC"} }

type QSelectable interface {
	GetSelectionSQL(qc *QContext) (string, error)
	GetResultName() string
}

func (qc QColumn) GetSelectionSQL(qctx *QContext) (string, error) {
	return qc.sqlName, nil
}
func (qc QColumn) GetResultName() string { return qc.Name }

type QAliasedExpression struct {
	Expression QSelectable
	AliasName  string
}

func (ae QAliasedExpression) GetSelectionSQL(qc *QContext) (string, error) {
	sql, err := ae.Expression.GetSelectionSQL(qc)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s AS %s", sql, ae.AliasName), nil
}
func (ae QAliasedExpression) GetResultName() string { return ae.AliasName }

type QAllColumns struct {
	Table *QTable
}

func (ac QAllColumns) GetSelectionSQL(qc *QContext) (string, error) {
	if ac.Table != nil {
		return fmt.Sprintf("%s.*", ac.Table.Name), nil
	}
	return "*", nil
}
func (ac QAllColumns) GetResultName() string { return "*" }

type QCondition interface {
	ToSQL(qc *QContext) (string, error)
}

type QBinaryCondition struct {
	Left     QColumn
	Operator string
	Right    any
}

func (bc QBinaryCondition) ToSQL(qc *QContext) (string, error) {
	leftSQL, _ := bc.Left.GetSelectionSQL(qc)
	placeholder := qc.addParam(bc.Right) // addParam now handles UUID
	return fmt.Sprintf("%s %s %s", leftSQL, bc.Operator, placeholder), nil
}

func Eq(column QColumn, value any) QCondition {
	return QBinaryCondition{Left: column, Operator: "=", Right: value}
}

type QOrderTerm struct {
	Column    QColumn
	Direction string
}
