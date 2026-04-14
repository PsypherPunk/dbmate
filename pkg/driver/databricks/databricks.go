package databricks

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strings"

	// Register Databricks driver.
	_ "github.com/databricks/databricks-sql-go"

	"github.com/amacneil/dbmate/v2/pkg/dbmate"
	"github.com/amacneil/dbmate/v2/pkg/dbutil"
)

func init() {
	dbmate.RegisterDriver(NewDriver, "databricks")
}

type Driver struct {
	migrationsTableName string
	databaseURL         *url.URL
	log                 io.Writer
}

func NewDriver(config dbmate.DriverConfig) dbmate.Driver {
	return &Driver{
		migrationsTableName: config.MigrationsTableName,
		databaseURL:         config.DatabaseURL,
		log:                 config.Log,
	}
}

func (drv *Driver) catalogName() string {
	catalog := drv.databaseURL.Query().Get("catalog")
	if catalog == "" {
		return "main"
	}
	return catalog
}

func (drv *Driver) schemaName() string {
	schema := drv.databaseURL.Query().Get("schema")
	if schema == "" {
		return "default"
	}
	return schema
}

func quoteIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func (drv *Driver) qualifiedMigrationsTableName() string {
	return fmt.Sprintf("%s.%s.%s",
		quoteIdentifier(drv.catalogName()),
		quoteIdentifier(drv.schemaName()),
		quoteIdentifier(drv.migrationsTableName),
	)
}

func connectionDSN(u *url.URL) string {
	cloned, _ := url.Parse(u.String())

	var dsn strings.Builder

	if cloned.User != nil {
		dsn.WriteString(cloned.User.String())
		dsn.WriteString("@")
	}

	dsn.WriteString(cloned.Host)
	dsn.WriteString(cloned.Path)

	query := cloned.Query()
	if query.Encode() != "" {
		dsn.WriteString("?")
		dsn.WriteString(query.Encode())
	}

	return dsn.String()
}

func (drv *Driver) Open() (*sql.DB, error) {
	return sql.Open("databricks", connectionDSN(drv.databaseURL))
}

func (drv *Driver) CreateDatabase() error {
	name := drv.schemaName()
	fmt.Fprintf(drv.log, "Creating: %s\n", name)

	db, err := drv.Open()
	if err != nil {
		return err
	}
	defer dbutil.MustClose(db)

	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s",
		quoteIdentifier(drv.catalogName()),
		quoteIdentifier(name)))

	return err
}

func (drv *Driver) DropDatabase() error {
	name := drv.schemaName()
	fmt.Fprintf(drv.log, "Dropping: %s\n", name)

	db, err := drv.Open()
	if err != nil {
		return err
	}
	defer dbutil.MustClose(db)

	_, err = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s CASCADE",
		quoteIdentifier(drv.catalogName()),
		quoteIdentifier(name)))

	return err
}

func (drv *Driver) DatabaseExists() (bool, error) {
	db, err := drv.Open()
	if err != nil {
		return false, err
	}
	defer dbutil.MustClose(db)

	var exists bool
	err = db.QueryRow(
		"SELECT 1 FROM system.information_schema.schemata WHERE catalog_name = ? AND schema_name = ?",
		drv.catalogName(), drv.schemaName(),
	).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}

	return exists, err
}

func (drv *Driver) MigrationsTableExists(db *sql.DB) (bool, error) {
	var exists bool
	err := db.QueryRow(
		"SELECT 1 FROM system.information_schema.tables WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
		drv.catalogName(), drv.schemaName(), drv.migrationsTableName,
	).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}

	return exists, err
}

func (drv *Driver) CreateMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (version VARCHAR(255) NOT NULL PRIMARY KEY)",
		drv.qualifiedMigrationsTableName()))

	return err
}

func (drv *Driver) SelectMigrations(db *sql.DB, limit int) (map[string]bool, error) {
	query := fmt.Sprintf("SELECT version FROM %s ORDER BY version DESC",
		drv.qualifiedMigrationsTableName())
	if limit >= 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, limit)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer dbutil.MustClose(rows)

	migrations := map[string]bool{}
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		migrations[version] = true
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return migrations, nil
}

func (drv *Driver) InsertMigration(db dbutil.Transaction, version string) error {
	_, err := db.Exec(
		fmt.Sprintf("INSERT INTO %s (version) VALUES (?)", drv.qualifiedMigrationsTableName()),
		version)

	return err
}

func (drv *Driver) DeleteMigration(db dbutil.Transaction, version string) error {
	_, err := db.Exec(
		fmt.Sprintf("DELETE FROM %s WHERE version = ?", drv.qualifiedMigrationsTableName()),
		version)

	return err
}

func (drv *Driver) Ping() error {
	db, err := drv.Open()
	if err != nil {
		return err
	}
	defer dbutil.MustClose(db)

	return db.Ping()
}

func (drv *Driver) schemaDump(db *sql.DB) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("\n--\n-- Database schema\n--\n\n")

	catalog := drv.catalogName()
	schema := drv.schemaName()

	rows, err := db.Query(
		"SELECT table_name FROM system.information_schema.tables WHERE table_catalog = ? AND table_schema = ? ORDER BY table_name",
		catalog, schema,
	)
	if err != nil {
		return nil, err
	}
	defer dbutil.MustClose(rows)

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	for _, table := range tables {
		qualifiedName := fmt.Sprintf("%s.%s.%s",
			quoteIdentifier(catalog),
			quoteIdentifier(schema),
			quoteIdentifier(table),
		)
		var stmt string
		err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", qualifiedName)).Scan(&stmt)
		if err != nil {
			return nil, err
		}
		buf.WriteString(stmt + ";\n\n")
	}

	return buf.Bytes(), nil
}

func (drv *Driver) schemaMigrationsDump(db *sql.DB) ([]byte, error) {
	migrationsTable := drv.qualifiedMigrationsTableName()

	migrations, err := dbutil.QueryColumn(db,
		fmt.Sprintf("SELECT version FROM %s ORDER BY version ASC", migrationsTable))
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.WriteString("\n--\n-- Dbmate schema migrations\n--\n\n")

	if len(migrations) > 0 {
		buf.WriteString(
			fmt.Sprintf("INSERT INTO %s (version) VALUES\n    ('", migrationsTable) +
				strings.Join(migrations, "'),\n    ('") +
				"');\n")
	}

	return buf.Bytes(), nil
}

func (drv *Driver) DumpSchema(db *sql.DB, _ ...string) ([]byte, error) {
	schema, err := drv.schemaDump(db)
	if err != nil {
		return nil, err
	}

	migrations, err := drv.schemaMigrationsDump(db)
	if err != nil {
		return nil, err
	}

	return append(schema, migrations...), nil
}

func (drv *Driver) QueryError(query string, err error) error {
	return &dbmate.QueryError{Err: err, Query: query}
}
