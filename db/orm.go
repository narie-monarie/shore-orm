package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid" // Keep for QContext addParam if still used there
)

// ANSI color codes (assuming they are defined in schema_types.go or here)

// ORM is the main entry point for database operations.
type ORM struct {
	Db *sql.DB
}

// NewORM creates a new ORM instance.
func NewORM(db *sql.DB) *ORM {
	return &ORM{Db: db}
}

// TableOptions configure table creation behavior.
type TableOptions struct {
	DropIfExists         bool
	IgnoreExists         bool
	DropIfExistsAndEmpty bool
}

// ExistingColumnInfo holds information about a column fetched from the database.
// This is an internal type for schema synchronization.
type existingColumnInfo struct { // Made unexported as it's an internal detail
	Name       string
	Type       string
	Length     sql.NullInt64
	Precision  sql.NullInt64
	Scale      sql.NullInt64
	IsNullable bool
	Default    sql.NullString
}

// tableExists checks if a table exists in the database.
func (o *ORM) tableExists(ctx context.Context, tableName string) (bool, error) {
	var count int
	upperTableName := strings.ToUpper(tableName)
	query := fmt.Sprintf(`SELECT COUNT(*) FROM user_tables WHERE table_name = '%s'`, upperTableName)
	err := o.Db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("error executing table existence check for %s: %w", upperTableName, err)
	}
	return count > 0, nil
}

// tableIsEmpty checks if a table has any rows.
func (o *ORM) tableIsEmpty(ctx context.Context, tableName string) (bool, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s WHERE ROWNUM = 1)", strings.ToUpper(tableName))
	err := o.Db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		if oraErr, ok := err.(interface{ Code() int }); ok && oraErr.Code() == 942 {
			return true, nil
		}
		return false, fmt.Errorf("checking if table %s is empty: %w", tableName, err)
	}
	return count == 0, nil
}

// CreateTable creates a table if it doesn't exist or handles dropping based on options.
func (o *ORM) CreateTable(ctx context.Context, schema TableSchema, opts ...TableOptions) error {
	var options TableOptions
	if len(opts) > 0 {
		options = opts[0]
	}

	exists, err := o.tableExists(ctx, schema.Name)
	if err != nil {
		fmt.Printf("%sWarning: could not determine if table %s exists: %v%s\n", ColorYellow, schema.Name, err, ColorReset)
	}

	shouldDrop := false
	if exists {
		if options.DropIfExists {
			shouldDrop = true
		} else if options.DropIfExistsAndEmpty {
			isEmpty, emptyCheckErr := o.tableIsEmpty(ctx, schema.Name)
			if emptyCheckErr != nil {
				fmt.Printf("%sWarning: could not determine if table %s is empty for DropIfExistsAndEmpty: %v. Will not drop based on this rule.%s\n", ColorYellow, schema.Name, emptyCheckErr, ColorReset)
			} else if isEmpty {
				shouldDrop = true
				fmt.Printf("%sTable %s is empty. Will be dropped and recreated.%s\n", ColorYellow, schema.Name, ColorReset)
			} else {
				fmt.Printf("%sTable %s exists and is not empty. Skipping drop due to DropIfExistsAndEmpty option.%s\n", ColorYellow, schema.Name, ColorReset)
			}
		}
	}

	if shouldDrop {
		dropSQL := fmt.Sprintf("DROP TABLE %s CASCADE CONSTRAINTS", schema.Name)
		fmt.Printf("Attempting to drop table %s: %s%s%s\n", schema.Name, ColorCyan, dropSQL, ColorReset) // Single line
		_, execErr := o.Db.ExecContext(ctx, dropSQL)
		if execErr != nil {
			if oraErr, ok := execErr.(interface{ Code() int }); ok && oraErr.Code() == 942 {
				fmt.Printf("%sTable %s did not exist (or was already dropped).%s\n", ColorYellow, schema.Name, ColorReset)
			} else {
				fmt.Printf("%sWarning: error dropping table %s: %v%s\n", ColorYellow, schema.Name, execErr, ColorReset)
			}
		} else {
			fmt.Printf("%sTable %s dropped successfully.%s\n", ColorGreen, schema.Name, ColorReset)
		}
		exists = false
	}

	if !exists {
		createSQL := o.buildCreateTableSQL(schema)
		fmt.Printf("Executing SQL for table %s: %s%s%s\n", schema.Name, ColorCyan, createSQL, ColorReset) // Single line
		_, createErr := o.Db.ExecContext(ctx, createSQL)
		if createErr != nil {
			return fmt.Errorf("%sfailed to create table %s: %w%s", ColorRed, schema.Name, createErr, ColorReset)
		}
		fmt.Printf("%sTable %s created successfully.%s\n", ColorGreen, schema.Name, ColorReset)
	} else {
		if options.IgnoreExists {
			fmt.Printf("%sTable %s already exists. Skipping creation due to IgnoreExists option.%s\n", ColorYellow, schema.Name, ColorReset)
			return nil
		}
		fmt.Printf("%sTable %s already exists and was not dropped. Will proceed to SyncSchema if defined.%s\n", ColorYellow, schema.Name, ColorReset)
	}

	if !exists || shouldDrop {
		for _, index := range schema.Indexes {
			indexSQL := o.buildCreateIndexSQL(schema.Name, index)
			fmt.Printf("Executing SQL for index %s on %s: %s%s%s\n", index.Name, schema.Name, ColorCyan, indexSQL, ColorReset) // Single line
			_, iErr := o.Db.ExecContext(ctx, indexSQL)
			if iErr != nil {
				if oraErr, ok := iErr.(interface{ Code() int }); ok && (oraErr.Code() == 955 || oraErr.Code() == 1408) {
					fmt.Printf("%sWarning: index %s on table %s likely already exists: %v%s\n", ColorYellow, index.Name, schema.Name, iErr, ColorReset)
				} else {
					return fmt.Errorf("%sfailed to create index %s: %w%s", ColorRed, index.Name, iErr, ColorReset)
				}
			}
		}
	}
	return nil
}

// getTableColumns introspects the database for existing columns of a table. (unexported)
func (o *ORM) getTableColumns(ctx context.Context, tableName string) (map[string]existingColumnInfo, error) {
	query := `
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            NVL(CHAR_COL_DECL_LENGTH, DATA_LENGTH) as EFFECTIVE_LENGTH,
            DATA_PRECISION,
            DATA_SCALE,
            NULLABLE,
            DATA_DEFAULT
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = :1
        ORDER BY COLUMN_ID
    `
	rows, err := o.Db.QueryContext(ctx, query, strings.ToUpper(tableName))
	if err != nil {
		return nil, fmt.Errorf("querying columns for table %s: %w", tableName, err)
	}
	defer rows.Close()

	existingCols := make(map[string]existingColumnInfo)
	for rows.Next() {
		var col existingColumnInfo
		var nullableStr string
		var effectiveLength sql.NullInt64

		err := rows.Scan(
			&col.Name,
			&col.Type,
			&effectiveLength,
			&col.Precision,
			&col.Scale,
			&nullableStr,
			&col.Default,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning column info for table %s: %w", tableName, err)
		}
		col.IsNullable = (nullableStr == "Y")
		col.Length = effectiveLength
		existingCols[col.Name] = col
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating column info for table %s: %w", tableName, err)
	}
	return existingCols, nil
}

// SyncSchema attempts to add missing columns from the Go schema definition to the database table.
func (o *ORM) SyncSchema(ctx context.Context, schema TableSchema) error {
	fmt.Printf("%sSyncing schema for table %s...%s\n", ColorYellow, schema.Name, ColorReset)

	exists, err := o.tableExists(ctx, schema.Name)
	if err != nil {
		return fmt.Errorf("could not check existence of table %s for sync: %w", schema.Name, err)
	}

	if !exists {
		fmt.Printf("%sTable %s does not exist. Cannot sync. Please use CreateTable first.%s\n", ColorRed, schema.Name, ColorReset)
		return fmt.Errorf("table %s does not exist, cannot sync columns", schema.Name)
	}

	existingDBCols, err := o.getTableColumns(ctx, schema.Name) // Use unexported getTableColumns
	if err != nil {
		return fmt.Errorf("could not get existing columns for table %s: %w", schema.Name, err)
	}

	var columnsToAdd []ColumnDef
	for _, desiredCol := range schema.Columns {
		if _, found := existingDBCols[desiredCol.Name]; !found {
			columnsToAdd = append(columnsToAdd, desiredCol)
		}
	}

	if len(columnsToAdd) == 0 {
		fmt.Printf("%sSchema for table %s appears up to date (no missing columns found).%s\n", ColorGreen, schema.Name, ColorReset)
		return nil
	}

	var addClauses []string
	for _, colDef := range columnsToAdd {
		colSQL := o.buildColumnDefinitionForAlter(colDef)
		addClauses = append(addClauses, colSQL)
		fmt.Printf("%sPlanning to add column to %s: %s%s\n", ColorCyan, schema.Name, colSQL, ColorReset)
	}

	alterSQL := fmt.Sprintf("ALTER TABLE %s ADD (%s)",
		schema.Name,
		strings.Join(addClauses, ", "),
	)

	fmt.Printf("%sExecuting Alter Table (Add Columns): %s%s%s\n", ColorGreen, ColorCyan, alterSQL, ColorReset) // Single line
	_, execErr := o.Db.ExecContext(ctx, alterSQL)
	if execErr != nil {
		return fmt.Errorf("failed to add columns to table %s: %w", schema.Name, execErr)
	}

	fmt.Printf("%sColumns added successfully to %s.%s\n", ColorGreen, schema.Name, ColorReset)
	return nil
}

func (o *ORM) buildColumnDefinitionForAlter(col ColumnDef) string {
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
		fmt.Printf("%sWarning: Inline FK definition for new column '%s' in ALTER TABLE is complex; add FK as separate constraint.%s\n", ColorYellow, col.Name, ColorReset)
	}
	if col.IsPrimaryKey {
		fmt.Printf("%sWarning: Inline PK definition for new column '%s' in ALTER TABLE is not standard.%s\n", ColorYellow, col.Name, ColorReset)
	}
	return strings.Join(parts, " ")
}
func (o *ORM) buildCreateTableSQL(schema TableSchema) string {
	var columnDefs, primaryKeyCols, tableLevelConstraints []string
	hasInlinePK := false
	for _, col := range schema.Columns {
		colSQL, isColInlinePK := o.buildColumnDefinition(col)
		columnDefs = append(columnDefs, colSQL)
		if isColInlinePK {
			hasInlinePK = true
		} else if col.IsPrimaryKey {
			primaryKeyCols = append(primaryKeyCols, col.Name)
		}
	}
	if !hasInlinePK && len(primaryKeyCols) > 0 {
		tableLevelConstraints = append(tableLevelConstraints, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeyCols, ", ")))
	}
	sql := fmt.Sprintf("CREATE TABLE %s (\n  %s", schema.Name, strings.Join(columnDefs, ",\n  "))
	if len(tableLevelConstraints) > 0 {
		sql += ",\n  " + strings.Join(tableLevelConstraints, ",\n  ")
	}
	sql += "\n)"
	return sql
}
func (o *ORM) buildColumnDefinition(col ColumnDef) (string, bool) {
	var parts []string
	parts = append(parts, col.Name)
	colTypeUpper := strings.ToUpper(col.Type)
	if col.Size > 0 && (strings.Contains(colTypeUpper, "VARCHAR") || strings.Contains(colTypeUpper, "CHAR")) {
		parts = append(parts, fmt.Sprintf("%s(%d)", col.Type, col.Size))
	} else {
		parts = append(parts, col.Type)
	}
	isInlinePK := false
	// This part is for Oracle Identity Columns if you re-add .Identity() to ColumnBuilder
	// and ColumnDef gets an IsIdentity flag.
	// if col.IsIdentity && col.IsPrimaryKey {
	// parts = append(parts, "GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY")
	// isInlinePK = true
	// }
	if col.Default != "" { // && !col.IsIdentity (if IsIdentity flag exists)
		defaultUpper := strings.ToUpper(col.Default)
		isKeywordDefault := defaultUpper == "CURRENT_TIMESTAMP" || defaultUpper == "SYSTIMESTAMP" || defaultUpper == "SYS_GUID()"
		needsQuotes := (strings.Contains(colTypeUpper, "VARCHAR") || strings.Contains(colTypeUpper, "CHAR")) || ((strings.Contains(colTypeUpper, "TIMESTAMP") || strings.Contains(colTypeUpper, "DATE")) && !isKeywordDefault)
		if !isKeywordDefault && needsQuotes {
			parts = append(parts, "DEFAULT", fmt.Sprintf("'%s'", col.Default))
		} else {
			parts = append(parts, "DEFAULT", col.Default)
		}
	}
	if col.IsNotNull && !isInlinePK {
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
		if fk.OnUpdate != "" {
			fkSQL += " ON UPDATE " + strings.ToUpper(fk.OnUpdate)
		}
		parts = append(parts, fkSQL)
	}
	return strings.Join(parts, " "), isInlinePK
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

// --- CRUD Builders (Single Line SQL Logging) ---
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
	fmt.Printf("%sExecuting Insert: %s%s%s Args: %v%s\n", ColorGreen, ColorCyan, query, ColorReset, args, ColorReset)
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
	fmt.Printf("%sExecuting Select: %s%s%s Args: %v%s\n", ColorGreen, ColorCyan, query, ColorReset, args, ColorReset)
	rows, err := sb.orm.Db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	colTypes, _ := rows.ColumnTypes()
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
				isLikelyUUID := false
				if colTypes != nil && i < len(colTypes) {
					dbTypeName := strings.ToUpper(colTypes[i].DatabaseTypeName())
					if dbTypeName == "RAW" && len(b) == 16 {
						isLikelyUUID = true
					}
				}
				if isLikelyUUID {
					parsedUUID, uuidErr := uuid.FromBytes(b)
					if uuidErr == nil {
						entry[colName] = parsedUUID.String()
					} else {
						entry[colName] = fmt.Sprintf("%x (uuid parse error: %v)", b, uuidErr)
					}
				} else {
					entry[colName] = fmt.Sprintf("%x", b)
				}
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
	fmt.Printf("%sExecuting Select Scalar: %s%s%s Args: %v%s\n", ColorGreen, ColorCyan, query, ColorReset, args, ColorReset)
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
	fmt.Printf("%sExecuting Update: %s%s%s Args: %v%s\n", ColorGreen, ColorCyan, query, ColorReset, args, ColorReset)
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
	fmt.Printf("%sExecuting Delete: %s%s%s Args: %v%s\n", ColorGreen, ColorCyan, query, ColorReset, args, ColorReset)
	return dbuilder.orm.Db.ExecContext(ctx, query, args...)
}
