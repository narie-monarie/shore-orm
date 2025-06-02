package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

/*  Need to break down this file tho */
type ORM struct {
	Db *sql.DB
}

func NewORM(db *sql.DB) *ORM {
	return &ORM{Db: db}
}

type TableOptions struct {
	DropIfExists         bool
	IgnoreExists         bool
	DropIfExistsAndEmpty bool
}

func (o *ORM) tableIsEmpty(ctx context.Context, tableName string) (bool, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s WHERE ROWNUM = 1)", strings.ToUpper(tableName))
	err := o.Db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		if oraErr, ok := err.(interface{ Code() int }); ok && oraErr.Code() == 942 {
			return true, nil // Table doesn't exist, so it's "empty" for this purpose
		}
		return false, fmt.Errorf("checking if table %s is empty: %w", tableName, err)
	}
	return count == 0, nil
}

func (o *ORM) CreateTable(ctx context.Context, schema TableSchema, opts ...TableOptions) error {
	var options TableOptions
	if len(opts) > 0 {
		options = opts[0]
	}

	shouldDrop := options.DropIfExists
	if options.DropIfExistsAndEmpty {
		isEmpty, err := o.tableIsEmpty(ctx, schema.Name)
		if err != nil {
			fmt.Printf("%sWarning: could not determine if table %s is empty: %v. Defaulting to not drop based on this rule.%s\n", ColorYellow, schema.Name, err, ColorReset)
		} else if isEmpty {
			shouldDrop = true
			fmt.Printf("%sTable %s is empty or does not exist. Will proceed with drop if configured.%s\n", ColorYellow, schema.Name, ColorReset)
		} else { // Not empty
			fmt.Printf("%sTable %s exists and is not empty. Skipping drop due to DropIfExistsAndEmpty option.%s\n", ColorYellow, schema.Name, ColorReset)
			if !options.DropIfExists {
				shouldDrop = false
			} else { // DropIfExists was true, but DropIfExistsAndEmpty is also true and table is NOT empty
				shouldDrop = false // DropIfExistsAndEmpty
			}
		}
	}

	if shouldDrop {
		dropSQL := fmt.Sprintf("DROP TABLE %s CASCADE CONSTRAINTS", schema.Name)
		fmt.Printf("Attempting to drop table %s: %s%s%s\n", schema.Name, ColorCyan, dropSQL, ColorReset)
		_, err := o.Db.ExecContext(ctx, dropSQL)
		if err != nil {
			if oraErr, ok := err.(interface{ Code() int }); ok && oraErr.Code() == 942 {
				fmt.Printf("%sTable %s did not exist, skipping drop.%s\n", ColorYellow, schema.Name, ColorReset)
			} else {
				fmt.Printf("%sWarning: error dropping table %s: %v%s\n", ColorYellow, schema.Name, err, ColorReset)
			}
		} else {
			fmt.Printf("%sTable %s dropped successfully.%s\n", ColorGreen, schema.Name, ColorReset)
		}
	} else if options.IgnoreExists {
		//JUST MY THOUGHTS

		/* If not dropping and IgnoreExists is true, we might want to check if table exists
		and return nil if it does. For simplicity, this is omitted here, assuming
		the CREATE TABLE will fail if it exists and IgnoreExists is not fully handled.
		A proper tableExists check would be:
		exists, _ := o.tableExists(ctx, schema.Name) // You'd need a tableExists method
		if exists { return nil } */
	}

	createSQL := o.buildCreateTableSQL(schema)
	fmt.Printf("Executing SQL for table %s:\n%s%s%s\n", schema.Name, ColorCyan, createSQL, ColorReset)
	_, err := o.Db.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("%sfailed to create table %s: %w%s", ColorRed, schema.Name, err, ColorReset)
	}

	for _, index := range schema.Indexes {
		indexSQL := o.buildCreateIndexSQL(schema.Name, index)
		fmt.Printf("Executing SQL for index %s on %s:\n%s%s%s\n", index.Name, schema.Name, ColorCyan, indexSQL, ColorReset)
		_, iErr := o.Db.ExecContext(ctx, indexSQL)
		if iErr != nil {
			if oraErr, ok := iErr.(interface{ Code() int }); ok && (oraErr.Code() == 955 || oraErr.Code() == 1408) {
				fmt.Printf("%sWarning: index %s on table %s likely already exists: %v%s\n", ColorYellow, index.Name, schema.Name, iErr, ColorReset)
			} else {
				return fmt.Errorf("%sfailed to create index %s: %w%s", ColorRed, index.Name, iErr, ColorReset)
			}
		}
	}
	return nil
}

func (o *ORM) buildCreateTableSQL(schema TableSchema) string {
	var columnDefs, primaryKeyCols, tableLevelConstraints []string
	for _, col := range schema.Columns {
		colSQL := o.buildColumnDefinition(col)
		columnDefs = append(columnDefs, colSQL)
		if col.IsPrimaryKey {
			primaryKeyCols = append(primaryKeyCols, col.Name)
		}
	}
	if len(primaryKeyCols) > 0 {
		tableLevelConstraints = append(tableLevelConstraints, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeyCols, ", ")))
	}
	sql := fmt.Sprintf("CREATE TABLE %s (\n  %s", schema.Name, strings.Join(columnDefs, ",\n  "))
	if len(tableLevelConstraints) > 0 {
		sql += ",\n  " + strings.Join(tableLevelConstraints, ",\n  ")
	}
	sql += "\n)"
	return sql
}

func (o *ORM) buildColumnDefinition(col ColumnDef) string {
	var parts []string
	parts = append(parts, col.Name)
	colTypeUpper := strings.ToUpper(col.Type)
	if col.Size > 0 && (strings.Contains(colTypeUpper, "VARCHAR") || strings.Contains(colTypeUpper, "CHAR")) {
		parts = append(parts, fmt.Sprintf("%s(%d)", col.Type, col.Size))
	} else {
		parts = append(parts, col.Type)
	}
	if col.Default != "" {
		defaultUpper := strings.ToUpper(col.Default)
		isKeywordDefault := defaultUpper == "CURRENT_TIMESTAMP" || defaultUpper == "SYSTIMESTAMP" || defaultUpper == "SYS_GUID()"
		needsQuotes := (strings.Contains(colTypeUpper, "VARCHAR") || strings.Contains(colTypeUpper, "CHAR")) || ((strings.Contains(colTypeUpper, "TIMESTAMP") || strings.Contains(colTypeUpper, "DATE")) && !isKeywordDefault)
		if !isKeywordDefault && needsQuotes {
			parts = append(parts, "DEFAULT", fmt.Sprintf("'%s'", col.Default))
		} else {
			parts = append(parts, "DEFAULT", col.Default)
		}
	}
	if col.IsNotNull {
		parts = append(parts, "NOT NULL")
	}
	if col.IsUnique {
		parts = append(parts, "UNIQUE")
	}
	if col.ForeignKey != nil {
		fk := col.ForeignKey
		fkSQL := fmt.Sprintf("REFERENCES %s(%s)", strings.ToUpper(fk.ReferencedTable), strings.ToUpper(fk.ReferencedColumn))
		if fk.OnDelete != "" {
			fkSQL += " ON DELETE " + strings.ToUpper(fk.OnDelete)
		}
		if fk.OnUpdate != "" { // Oracle ON UPDATE caveat
			fkSQL += " ON UPDATE " + strings.ToUpper(fk.OnUpdate)
		}
		parts = append(parts, fkSQL)
	}
	return strings.Join(parts, " ")
}

func (o *ORM) buildCreateIndexSQL(tableName string, index IndexDef) string {
	indexType := "INDEX"
	if index.IsUnique {
		indexType = "UNIQUE INDEX"
	}
	return fmt.Sprintf("CREATE %s %s ON %s (%s)", indexType, index.Name, tableName, strings.Join(index.Columns, ", "))
}

func resolveTableName(identifier any) (string, error) {
	var tableName string
	switch t := identifier.(type) {
	case TableSchema:
		tableName = t.Name
	case *TableSchema:
		tableName = t.Name
	case QTable:
		tableName = t.Name
	case *QTable:
		tableName = t.Name
	case string:
		tableName = strings.ToUpper(t)
	default:
		return "", fmt.Errorf("unsupported table identifier type: %T", identifier)
	}
	if tableName == "" {
		return "", errors.New("table identifier resolved to empty name")
	}
	return tableName, nil
}

type InsertBuilder struct {
	orm    *ORM
	table  string
	values []map[string]any
}

func (o *ORM) InsertInto(tableIdentifier any) *InsertBuilder {
	tableName, err := resolveTableName(tableIdentifier)
	if err != nil {
		panic(fmt.Sprintf("InsertInto: %v", err))
	}
	return &InsertBuilder{orm: o, table: tableName}
}

func (ib *InsertBuilder) Values(data ...map[string]any) *InsertBuilder {
	ib.values = append(ib.values, data...)
	return ib
}

func (ib *InsertBuilder) ToSQL() (string, []any, error) {
	if len(ib.values) == 0 {
		return "", nil, errors.New("no values for insert")
	}
	rowData := ib.values[0]
	qc := newQContext("oracle")
	var columns, placeholders []string
	for colName, val := range rowData {
		columns = append(columns, strings.ToUpper(colName))
		placeholders = append(placeholders, qc.addParam(val))
	}
	if len(columns) == 0 {
		return "", nil, errors.New("no columns in values map")
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", ib.table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	return sql, qc.params, nil
}

func (ib *InsertBuilder) Exec(ctx context.Context) (sql.Result, error) {
	query, args, err := ib.ToSQL()
	if err != nil {
		return nil, err
	}
	fmt.Printf("%sExecuting Insert:%s\n%s%s%s\nArgs: %v\n", ColorGreen, ColorReset, ColorCyan, query, ColorReset, args)
	return ib.orm.Db.ExecContext(ctx, query, args...)
}

type SelectBuilder struct {
	orm           *ORM
	isDistinct    bool
	selections    []QSelectable
	fromTable     string
	conditions    []QCondition
	orderBy       []QOrderTerm
	limit, offset *int
}

func (o *ORM) Select(selections ...QSelectable) *SelectBuilder {
	return &SelectBuilder{orm: o, selections: selections}
}
func (sb *SelectBuilder) From(tableIdentifier any) *SelectBuilder {
	tableName, err := resolveTableName(tableIdentifier)
	if err != nil {
		panic(fmt.Sprintf("SelectBuilder.From: %v", err))
	}
	sb.fromTable = tableName
	if len(sb.selections) == 0 {
		var baseQTable QTable
		switch t := tableIdentifier.(type) {
		case TableSchema:
			baseQTable = t.Q()
		case *TableSchema:
			baseQTable = t.Q()
		case QTable:
			baseQTable = t
		case *QTable:
			baseQTable = *t
		default:
			baseQTable = QTable{Name: tableName}
		}
		sb.selections = []QSelectable{QAllColumns{Table: &baseQTable}}
	}
	return sb
}

func (sb *SelectBuilder) Where(conditions ...QCondition) *SelectBuilder {
	sb.conditions = append(sb.conditions, conditions...)
	return sb
}

func (sb *SelectBuilder) OrderBy(terms ...QOrderTerm) *SelectBuilder {
	sb.orderBy = append(sb.orderBy, terms...)
	return sb
}

func (sb *SelectBuilder) Limit(count int) *SelectBuilder { sb.limit = &count; return sb }

func (sb *SelectBuilder) Offset(skip int) *SelectBuilder { sb.offset = &skip; return sb }

func (sb *SelectBuilder) ToSQL() (string, []any, error) {
	if sb.fromTable == "" {
		return "", nil, errors.New("from table not specified")
	}
	qc := newQContext("oracle")
	var selectParts []string
	for _, sel := range sb.selections {
		s, err := sel.GetSelectionSQL(qc)
		if err != nil {
			return "", nil, err
		}
		selectParts = append(selectParts, s)
	}
	sqlStr := "SELECT "
	if sb.isDistinct {
		sqlStr += "DISTINCT "
	}
	sqlStr += strings.Join(selectParts, ", ") + " FROM " + sb.fromTable
	if len(sb.conditions) > 0 {
		var whereParts []string
		for _, cond := range sb.conditions {
			s, err := cond.ToSQL(qc)
			if err != nil {
				return "", nil, err
			}
			whereParts = append(whereParts, "("+s+")")
		}
		sqlStr += " WHERE " + strings.Join(whereParts, " AND ")
	}
	if len(sb.orderBy) > 0 {
		var orderSQLs []string
		for _, term := range sb.orderBy {
			colSql, _ := term.Column.GetSelectionSQL(qc)
			orderSQLs = append(orderSQLs, fmt.Sprintf("%s %s", colSql, term.Direction))
		}
		sqlStr += " ORDER BY " + strings.Join(orderSQLs, ", ")
	}
	if sb.limit != nil || sb.offset != nil {
		if sb.offset != nil {
			sqlStr += " OFFSET " + qc.addParam(*sb.offset) + " ROWS"
		}
		if sb.limit != nil {
			sqlStr += " FETCH NEXT " + qc.addParam(*sb.limit) + " ROWS ONLY"
		}
	}
	return sqlStr, qc.params, nil
}

func (sb *SelectBuilder) QueryMaps(ctx context.Context) ([]map[string]any, error) {
	query, args, err := sb.ToSQL()
	if err != nil {
		return nil, err
	}
	fmt.Printf("%sExecuting Select:%s\n%s%s%s\nArgs: %v\n", ColorGreen, ColorReset, ColorCyan, query, ColorReset, args)
	rows, err := sb.orm.Db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		entry := make(map[string]any)
		for i, colName := range columns {
			var val = values[i]
			if b, ok := val.([]byte); ok {
				entry[colName] = fmt.Sprintf("%x", b)
			} else {
				entry[colName] = val
			}
		}
		results = append(results, entry)
	}
	return results, rows.Err()
}

func (sb *SelectBuilder) QueryScalar(ctx context.Context, dest any) error {
	query, args, err := sb.ToSQL()
	if err != nil {
		return err
	}
	fmt.Printf("%sExecuting Select Scalar:%s\n%s%s%s\nArgs: %v\n", ColorGreen, ColorReset, ColorCyan, query, ColorReset, args)
	row := sb.orm.Db.QueryRowContext(ctx, query, args...)
	return row.Scan(dest)
}

type UpdateBuilder struct {
	orm        *ORM
	table      string
	setValues  map[string]any
	conditions []QCondition
}

func (o *ORM) Update(tableIdentifier any) *UpdateBuilder {
	tableName, err := resolveTableName(tableIdentifier)
	if err != nil {
		panic(fmt.Sprintf("Update: %v", err))
	}
	return &UpdateBuilder{orm: o, table: tableName, setValues: make(map[string]any)}
}
func (ub *UpdateBuilder) Set(data map[string]any) *UpdateBuilder {
	for k, v := range data {
		ub.setValues[strings.ToUpper(k)] = v
	}
	return ub
}
func (ub *UpdateBuilder) Where(conditions ...QCondition) *UpdateBuilder {
	ub.conditions = append(ub.conditions, conditions...)
	return ub
}
func (ub *UpdateBuilder) ToSQL() (string, []any, error) {
	if len(ub.setValues) == 0 {
		return "", nil, errors.New("no values for update")
	}
	qc := newQContext("oracle")
	var setParts []string
	for col, val := range ub.setValues {
		placeholder := qc.addParam(val)
		setParts = append(setParts, fmt.Sprintf("%s = %s", col, placeholder))
	}
	sqlStr := fmt.Sprintf("UPDATE %s SET %s", ub.table, strings.Join(setParts, ", "))
	if len(ub.conditions) > 0 {
		var whereParts []string
		for _, cond := range ub.conditions {
			s, err := cond.ToSQL(qc)
			if err != nil {
				return "", nil, err
			}
			whereParts = append(whereParts, "("+s+")")
		}
		sqlStr += " WHERE " + strings.Join(whereParts, " AND ")
	}
	return sqlStr, qc.params, nil
}
func (ub *UpdateBuilder) Exec(ctx context.Context) (sql.Result, error) {
	query, args, err := ub.ToSQL()
	if err != nil {
		return nil, err
	}
	fmt.Printf("%sExecuting Update:%s\n%s%s%s\nArgs: %v\n", ColorGreen, ColorReset, ColorCyan, query, ColorReset, args)
	return ub.orm.Db.ExecContext(ctx, query, args...)
}

type DeleteBuilder struct {
	orm        *ORM
	fromTable  string
	conditions []QCondition
}

func (o *ORM) DeleteFrom(tableIdentifier any) *DeleteBuilder {
	tableName, err := resolveTableName(tableIdentifier)
	if err != nil {
		panic(fmt.Sprintf("DeleteFrom: %v", err))
	}
	return &DeleteBuilder{orm: o, fromTable: tableName}
}
func (dbuilder *DeleteBuilder) Where(conditions ...QCondition) *DeleteBuilder {
	dbuilder.conditions = append(dbuilder.conditions, conditions...)
	return dbuilder
}
func (dbuilder *DeleteBuilder) ToSQL() (string, []any, error) {
	qc := newQContext("oracle")
	sqlStr := fmt.Sprintf("DELETE FROM %s", dbuilder.fromTable)
	if len(dbuilder.conditions) > 0 {
		var whereParts []string
		for _, cond := range dbuilder.conditions {
			s, err := cond.ToSQL(qc)
			if err != nil {
				return "", nil, err
			}
			whereParts = append(whereParts, "("+s+")")
		}
		sqlStr += " WHERE " + strings.Join(whereParts, " AND ")
	}
	return sqlStr, qc.params, nil
}
func (dbuilder *DeleteBuilder) Exec(ctx context.Context) (sql.Result, error) {
	query, args, err := dbuilder.ToSQL()
	if err != nil {
		return nil, err
	}
	fmt.Printf("%sExecuting Delete:%s\n%s%s%s\nArgs: %v\n", ColorGreen, ColorReset, ColorCyan, query, ColorReset, args)
	return dbuilder.orm.Db.ExecContext(ctx, query, args...)
}
