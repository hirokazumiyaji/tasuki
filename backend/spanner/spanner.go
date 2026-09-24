package spanner

import (
	"context"
	"embed"
	"fmt"
	"os"
	"regexp"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend/hub"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:embed schema.sql
var schemaFS embed.FS

var dsnRE = regexp.MustCompile(`^projects/([^/]+)/instances/([^/]+)/databases/([^/]+)$`)

// Backend is the Cloud Spanner implementation of backend.Backend.
type Backend struct {
	client *spanner.Client
	dsn    string
	hub    *hub.Hub
}

// New opens a Spanner client. Set SPANNER_EMULATOR_HOST for the emulator.
// dsn example: projects/tasuki/instances/tasuki/databases/tasuki
func New(ctx context.Context, dsn string) (*Backend, error) {
	client, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("spanner: open: %w", err)
	}
	return &Backend{client: client, dsn: dsn, hub: hub.New()}, nil
}

func (b *Backend) Close() error {
	b.client.Close()
	return nil
}

func (b *Backend) Client() *spanner.Client { return b.client }

func (b *Backend) DSN() string { return b.dsn }

// EnsureDatabase creates the instance and database on the emulator (or no-ops if they exist).
// Call before New when using SPANNER_EMULATOR_HOST.
func EnsureDatabase(ctx context.Context, dsn string) error {
	project, inst, dbName, err := parseDSN(dsn)
	if err != nil {
		return err
	}
	if err := ensureInstance(ctx, project, inst); err != nil {
		return err
	}
	return ensureDatabase(ctx, project, inst, dbName)
}

// RecreateDatabase drops and recreates the database (emulator-friendly schema resets).
func RecreateDatabase(ctx context.Context, dsn string) error {
	project, inst, dbName, err := parseDSN(dsn)
	if err != nil {
		return err
	}
	if err := ensureInstance(ctx, project, inst); err != nil {
		return err
	}
	admin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer admin.Close()
	name := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, inst, dbName)
	err = admin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: name})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("spanner drop database: %w", err)
	}
	return ensureDatabase(ctx, project, inst, dbName)
}

func parseDSN(dsn string) (project, instanceID, databaseID string, err error) {
	m := dsnRE.FindStringSubmatch(dsn)
	if m == nil {
		return "", "", "", fmt.Errorf("spanner: invalid dsn %q (want projects/p/instances/i/databases/d)", dsn)
	}
	return m[1], m[2], m[3], nil
}

func ensureInstance(ctx context.Context, project, instanceID string) error {
	admin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("spanner instance admin: %w", err)
	}
	defer admin.Close()

	name := fmt.Sprintf("projects/%s/instances/%s", project, instanceID)
	_, err = admin.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: name})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return err
	}

	config := "emulator-config"
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		config = "regional-us-central1"
	}
	op, err := admin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", project),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/%s", project, config),
			DisplayName: instanceID,
			NodeCount:   1,
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return fmt.Errorf("spanner create instance: %w", err)
	}
	_, err = op.Wait(ctx)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("spanner create instance wait: %w", err)
	}
	return nil
}

func ensureDatabase(ctx context.Context, project, instanceID, databaseID string) error {
	admin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("spanner database admin: %w", err)
	}
	defer admin.Close()

	name := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instanceID, databaseID)
	_, err = admin.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: name})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return err
	}

	parent := fmt.Sprintf("projects/%s/instances/%s", project, instanceID)
	op, err := admin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          parent,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return fmt.Errorf("spanner create database: %w", err)
	}
	_, err = op.Wait(ctx)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("spanner create database wait: %w", err)
	}
	return nil
}

func (b *Backend) Migrate(ctx context.Context) error {
	exists, err := b.tableExists(ctx, "wf_instances")
	if err != nil {
		return err
	}
	if !exists {
		stmts, err := loadDDL()
		if err != nil {
			return err
		}
		if err := b.applyDDL(ctx, stmts); err != nil {
			return err
		}
	}
	dedupeExists, err := b.tableExists(ctx, "wf_signal_dedupe")
	if err != nil {
		return err
	}
	if !dedupeExists {
		if err := b.applyDDL(ctx, []string{`
CREATE TABLE wf_signal_dedupe (
  instance_id STRING(255) NOT NULL,
  dedupe_id STRING(255) NOT NULL,
  created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (instance_id, dedupe_id)`}); err != nil {
			return err
		}
	}
	seqExists, err := b.tableExists(ctx, "wf_inbox_seq")
	if err != nil {
		return err
	}
	if !seqExists {
		if err := b.applyDDL(ctx, []string{`
CREATE TABLE wf_inbox_seq (
  instance_id STRING(255) NOT NULL,
  seq INT64 NOT NULL
) PRIMARY KEY (instance_id)`}); err != nil {
			return err
		}
	}
	if err := b.ensureSearchAttributesColumn(ctx); err != nil {
		return err
	}
	if err := b.ensureInt64Column(ctx, "wf_inbox", "seq"); err != nil {
		return err
	}
	if err := b.ensureCommitTimestampOptions(ctx); err != nil {
		return err
	}
	return b.ensureTasksInstanceIndex(ctx)
}

func (b *Backend) ensureSearchAttributesColumn(ctx context.Context) error {
	if err := b.ensureJSONColumn(ctx, "search_attributes"); err != nil {
		return err
	}
	return b.ensureJSONColumn(ctx, "memo")
}

func (b *Backend) ensureJSONColumn(ctx context.Context, column string) error {
	exists, err := b.columnExists(ctx, "wf_instances", column)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return b.applyDDL(ctx, []string{fmt.Sprintf(`ALTER TABLE wf_instances ADD COLUMN %s JSON`, column)})
}

func (b *Backend) ensureInt64Column(ctx context.Context, table, column string) error {
	exists, err := b.columnExists(ctx, table, column)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return b.applyDDL(ctx, []string{fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s INT64`, table, column)})
}

// ensureCommitTimestampOptions backfills commit-timestamp support on the
// terminal-sweep ordering columns for databases created before it existed
// (Codex round-22 P2 on #291): sweep_commit_ts on wf_instances and
// created_at on wf_signal_dedupe, wf_tasks and wf_inbox must accept the
// commit-timestamp placeholder, so sweep cutoffs compare server commit
// order instead of writer wall clocks. Fresh databases get both from
// schema.sql; pre-existing ones gain them here: sweep_commit_ts is a new
// column (added), the created_at columns only gain the option (ALTERed).
// completed_at intentionally keeps NO option: it stays a client-wall-clock
// timestamp so retention purges compare client time against client time —
// a commit-tick completed_at can postdate the purger's clock (TrueTime
// uncertainty; the emulator's clock runs ~150ms ahead of clients), which
// would make zero-window purges miss fresh victims. The check reads
// INFORMATION_SCHEMA, so Migrate stays a no-op once set.
func (b *Backend) ensureCommitTimestampOptions(ctx context.Context) error {
	if err := b.ensureSweepCommitTsColumn(ctx); err != nil {
		return err
	}
	for _, c := range []struct{ table, column string }{
		{"wf_signal_dedupe", "created_at"},
		{"wf_tasks", "created_at"},
		{"wf_inbox", "created_at"},
	} {
		has, err := b.columnOptionAllowCommitTimestamp(ctx, c.table, c.column)
		if err != nil {
			return err
		}
		if has {
			continue
		}
		if err := b.applyDDL(ctx, []string{fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s SET OPTIONS (allow_commit_timestamp=true)`, c.table, c.column)}); err != nil {
			return err
		}
	}
	return nil
}

// ensureSweepCommitTsColumn adds the sweep_commit_ts ordering tick for
// databases created before it existed, then ensures it accepts the
// commit-timestamp placeholder (a fresh ADD COLUMN carries the option from
// the statement itself).
func (b *Backend) ensureSweepCommitTsColumn(ctx context.Context) error {
	exists, err := b.columnExists(ctx, "wf_instances", "sweep_commit_ts")
	if err != nil {
		return err
	}
	if !exists {
		return b.applyDDL(ctx, []string{`ALTER TABLE wf_instances ADD COLUMN sweep_commit_ts TIMESTAMP OPTIONS (allow_commit_timestamp=true)`})
	}
	has, err := b.columnOptionAllowCommitTimestamp(ctx, "wf_instances", "sweep_commit_ts")
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	return b.applyDDL(ctx, []string{`ALTER TABLE wf_instances ALTER COLUMN sweep_commit_ts SET OPTIONS (allow_commit_timestamp=true)`})
}

func (b *Backend) columnOptionAllowCommitTimestamp(ctx context.Context, table, column string) (bool, error) {
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT 1 FROM INFORMATION_SCHEMA.COLUMN_OPTIONS
			WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table AND COLUMN_NAME = @column AND OPTION_NAME = 'allow_commit_timestamp' LIMIT 1`,
		Params: map[string]any{"table": table, "column": column},
	})
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ensureTasksInstanceIndex backfills the instance_id index on wf_tasks for
// databases created before the index existed. Terminal cleanup lists one
// instance's tasks there; without the index that filter scans every task
// row in the database.
func (b *Backend) ensureTasksInstanceIndex(ctx context.Context) error {
	exists, err := b.indexExists(ctx, "wf_tasks", "wf_tasks_instance_idx")
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return b.applyDDL(ctx, []string{`CREATE INDEX wf_tasks_instance_idx ON wf_tasks(instance_id)`})
}

func (b *Backend) indexExists(ctx context.Context, table, index string) (bool, error) {
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT 1 FROM INFORMATION_SCHEMA.INDEXES
			WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table AND INDEX_NAME = @index LIMIT 1`,
		Params: map[string]any{"table": table, "index": index},
	})
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (b *Backend) columnExists(ctx context.Context, table, column string) (bool, error) {
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table AND COLUMN_NAME = @column LIMIT 1`,
		Params: map[string]any{"table": table, "column": column},
	})
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (b *Backend) applyDDL(ctx context.Context, stmts []string) error {
	admin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer admin.Close()

	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   b.dsn,
		Statements: stmts,
	})
	if err != nil {
		if isAlreadyExistsDDL(err) {
			return nil
		}
		return fmt.Errorf("spanner migrate: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		if isAlreadyExistsDDL(err) {
			return nil
		}
		return fmt.Errorf("spanner migrate wait: %w", err)
	}
	return nil
}

func (b *Backend) tableExists(ctx context.Context, table string) (bool, error) {
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT 1 FROM INFORMATION_SCHEMA.TABLES
			WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @name LIMIT 1`,
		Params: map[string]any{"name": table},
	})
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func isAlreadyExistsDDL(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "duplicate") ||
		status.Code(err) == codes.AlreadyExists
}

// Reset deletes all rows (test helper).
func (b *Backend) Reset(ctx context.Context) error {
	type keyQuery struct {
		table string
		sql   string
		kind  string // "string" | "int64" | "pair"
	}
	queries := []keyQuery{
		{table: "wf_schedules", sql: `SELECT id FROM wf_schedules`, kind: "string"},
		{table: "wf_timers", sql: `SELECT instance_id, seq FROM wf_timers`, kind: "pair"},
		{table: "wf_tasks", sql: `SELECT id FROM wf_tasks`, kind: "int64"},
		{table: "wf_inbox", sql: `SELECT id FROM wf_inbox`, kind: "int64"},
		{table: "wf_inbox_seq", sql: `SELECT instance_id FROM wf_inbox_seq`, kind: "string"},
		{table: "wf_journal", sql: `SELECT instance_id, seq FROM wf_journal`, kind: "pair"},
		{table: "wf_instances", sql: `SELECT id FROM wf_instances`, kind: "string"},
	}
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var muts []*spanner.Mutation
		for _, q := range queries {
			iter := txn.Query(ctx, spanner.Statement{SQL: q.sql})
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					iter.Stop()
					return err
				}
				switch q.kind {
				case "string":
					var id string
					if err := row.Columns(&id); err != nil {
						iter.Stop()
						return err
					}
					muts = append(muts, spanner.Delete(q.table, spanner.Key{id}))
				case "int64":
					var id int64
					if err := row.Columns(&id); err != nil {
						iter.Stop()
						return err
					}
					muts = append(muts, spanner.Delete(q.table, spanner.Key{id}))
				case "pair":
					var instanceID string
					var seq int64
					if err := row.Columns(&instanceID, &seq); err != nil {
						iter.Stop()
						return err
					}
					muts = append(muts, spanner.Delete(q.table, spanner.Key{instanceID, seq}))
				}
			}
			iter.Stop()
		}
		return txn.BufferWrite(muts)
	})
	return err
}

func loadDDL() ([]string, error) {
	raw, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return nil, err
	}
	return splitSQL(string(raw)), nil
}

func splitSQL(s string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(s, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "--") {
			continue
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
		if strings.HasSuffix(trim, ";") {
			stmt := strings.TrimSpace(cur.String())
			stmt = strings.TrimSuffix(stmt, ";")
			stmt = strings.TrimSpace(stmt)
			if stmt != "" {
				out = append(out, stmt)
			}
			cur.Reset()
		}
	}
	if stmt := strings.TrimSpace(cur.String()); stmt != "" {
		out = append(out, stmt)
	}
	return out
}
