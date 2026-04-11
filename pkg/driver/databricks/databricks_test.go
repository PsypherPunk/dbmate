package databricks

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/amacneil/dbmate/v2/pkg/dbmate"
	"github.com/amacneil/dbmate/v2/pkg/dbtest"
	"github.com/amacneil/dbmate/v2/pkg/dbutil"
)

func testDatabricksDriver(t *testing.T) *Driver {
	u := dbtest.GetenvURLOrSkip(t, "DATABRICKS_TEST_URL")
	drv, err := dbmate.New(u).Driver()
	require.NoError(t, err)

	return drv.(*Driver)
}

func prepTestDatabricksDB(t *testing.T) *Driver {
	drv := testDatabricksDriver(t)

	err := drv.DropDatabase()
	require.NoError(t, err)

	err = drv.CreateDatabase()
	require.NoError(t, err)

	return drv
}

func TestGetDriver(t *testing.T) {
	db := dbmate.New(dbtest.MustParseURL(t, "databricks://token:dapi1234@host.databricks.com:443/sql/1.0/endpoints/abc?catalog=main&schema=testdb"))
	drvInterface, err := db.Driver()
	require.NoError(t, err)

	drv, ok := drvInterface.(*Driver)
	require.True(t, ok)
	require.Equal(t, db.DatabaseURL.String(), drv.databaseURL.String())
	require.Equal(t, "schema_migrations", drv.migrationsTableName)
}

func TestConnectionDSN(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "full URL with catalog and schema",
			input:    "databricks://token:dapi1234@host.databricks.com:443/sql/1.0/endpoints/abc?catalog=main&schema=testdb",
			expected: "token:dapi1234@host.databricks.com:443/sql/1.0/endpoints/abc?catalog=main&schema=testdb",
		},
		{
			name:     "minimal URL",
			input:    "databricks://token:dapi1234@host.databricks.com:443/sql/1.0/endpoints/abc",
			expected: "token:dapi1234@host.databricks.com:443/sql/1.0/endpoints/abc",
		},
		{
			name:     "URL with extra params",
			input:    "databricks://token:dapi1234@host.databricks.com:443/sql/1.0/endpoints/abc?catalog=prod&schema=mydb&timeout=60",
			expected: "token:dapi1234@host.databricks.com:443/sql/1.0/endpoints/abc?catalog=prod&schema=mydb&timeout=60",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.input)
			require.NoError(t, err)
			actual := connectionDSN(u)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "`simple`"},
		{"with space", "`with space`"},
		{"with`backtick", "`with``backtick`"},
		{"schema_migrations", "`schema_migrations`"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.expected, quoteIdentifier(tt.input))
		})
	}
}

func TestCatalogName(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected string
	}{
		{
			name:     "explicit catalog",
			url:      "databricks://token:x@host:443/path?catalog=prod",
			expected: "prod",
		},
		{
			name:     "default catalog",
			url:      "databricks://token:x@host:443/path",
			expected: "main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			require.NoError(t, err)
			drv := &Driver{databaseURL: u}
			require.Equal(t, tt.expected, drv.catalogName())
		})
	}
}

func TestSchemaName(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected string
	}{
		{
			name:     "explicit schema",
			url:      "databricks://token:x@host:443/path?schema=mydb",
			expected: "mydb",
		},
		{
			name:     "default schema",
			url:      "databricks://token:x@host:443/path",
			expected: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			require.NoError(t, err)
			drv := &Driver{databaseURL: u}
			require.Equal(t, tt.expected, drv.schemaName())
		})
	}
}

func TestQualifiedMigrationsTableName(t *testing.T) {
	u, err := url.Parse("databricks://token:x@host:443/path?catalog=main&schema=testdb")
	require.NoError(t, err)

	drv := &Driver{
		databaseURL:         u,
		migrationsTableName: "schema_migrations",
	}
	require.Equal(t, "`main`.`testdb`.`schema_migrations`", drv.qualifiedMigrationsTableName())
}

func TestCreateDropDatabase(t *testing.T) {
	drv := testDatabricksDriver(t)

	err := drv.DropDatabase()
	require.NoError(t, err)

	exists, err := drv.DatabaseExists()
	require.NoError(t, err)
	require.False(t, exists)

	err = drv.CreateDatabase()
	require.NoError(t, err)

	exists, err = drv.DatabaseExists()
	require.NoError(t, err)
	require.True(t, exists)

	err = drv.CreateDatabase()
	require.NoError(t, err)

	err = drv.DropDatabase()
	require.NoError(t, err)

	exists, err = drv.DatabaseExists()
	require.NoError(t, err)
	require.False(t, exists)
}

func TestPing(t *testing.T) {
	drv := testDatabricksDriver(t)

	err := drv.Ping()
	require.NoError(t, err)
}

func TestMigrationsTable(t *testing.T) {
	drv := prepTestDatabricksDB(t)
	defer func() {
		_ = drv.DropDatabase()
	}()

	db, err := drv.Open()
	require.NoError(t, err)
	defer dbutil.MustClose(db)

	exists, err := drv.MigrationsTableExists(db)
	require.NoError(t, err)
	require.False(t, exists)

	err = drv.CreateMigrationsTable(db)
	require.NoError(t, err)

	exists, err = drv.MigrationsTableExists(db)
	require.NoError(t, err)
	require.True(t, exists)

	err = drv.CreateMigrationsTable(db)
	require.NoError(t, err)
}

func TestSelectInsertDeleteMigrations(t *testing.T) {
	drv := prepTestDatabricksDB(t)
	defer func() {
		_ = drv.DropDatabase()
	}()

	db, err := drv.Open()
	require.NoError(t, err)
	defer dbutil.MustClose(db)

	err = drv.CreateMigrationsTable(db)
	require.NoError(t, err)

	migrations, err := drv.SelectMigrations(db, -1)
	require.NoError(t, err)
	require.Empty(t, migrations)

	err = drv.InsertMigration(db, "20200101000000")
	require.NoError(t, err)

	err = drv.InsertMigration(db, "20200101000001")
	require.NoError(t, err)

	migrations, err = drv.SelectMigrations(db, -1)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		"20200101000000": true,
		"20200101000001": true,
	}, migrations)

	migrations, err = drv.SelectMigrations(db, 1)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		"20200101000001": true,
	}, migrations)

	err = drv.DeleteMigration(db, "20200101000001")
	require.NoError(t, err)

	migrations, err = drv.SelectMigrations(db, -1)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		"20200101000000": true,
	}, migrations)
}

func TestDumpSchema(t *testing.T) {
	drv := prepTestDatabricksDB(t)
	defer func() {
		_ = drv.DropDatabase()
	}()

	db, err := drv.Open()
	require.NoError(t, err)
	defer dbutil.MustClose(db)

	err = drv.CreateMigrationsTable(db)
	require.NoError(t, err)

	err = drv.InsertMigration(db, "20200101000000")
	require.NoError(t, err)

	schema, err := drv.DumpSchema(db)
	require.NoError(t, err)
	require.Contains(t, string(schema), "Dbmate schema migrations")
	require.Contains(t, string(schema), "20200101000000")
}
