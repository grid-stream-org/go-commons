package bqclient

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/matthew-collett/go-ctag/ctag"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	tableProjects        = "projects"
	tableContracts       = "contracts"
	tableDERMetadata     = "der_metadata"
	tableDERData         = "der_data"
	tableProjectAverages = "project_averages"
	tableUtilities       = "utilities"
)

var validTables = map[string]bool{
	tableProjects:        true,
	tableContracts:       true,
	tableDERMetadata:     true,
	tableDERData:         true,
	tableProjectAverages: true,
	tableUtilities:       true,
}

type BQClient interface {
	Put(ctx context.Context, table string, data any) error
	StreamPut(ctx context.Context, table string, data any) error
	StreamPutAll(ctx context.Context, inputs map[string][]any) error
	Query(ctx context.Context, query string, params []bigquery.QueryParameter) (*bigquery.RowIterator, error)
	QueryRow(ctx context.Context, query string, params []bigquery.QueryParameter, dst any) error
	Update(ctx context.Context, table string, id string, updates map[string]interface{}) error
	Delete(ctx context.Context, table string, id string) error
	Get(ctx context.Context, table string, id string, dst any) error
	Close() error
}

type Config struct {
	ProjectID string `koanf:"project_id" json:"project_id" envconfig:"project_id"`
	DatasetID string `koanf:"dataset_id" json:"dataset_id" envconfig:"dataset_id"`
	CredsPath string `koanf:"creds_path" json:"creds_path" envconfig:"creds_path"`
}

type bqClient struct {
	cfg    *Config
	client *bigquery.Client
}

var (
	errInvalidTable = errors.New("invalid table name")
	ErrNotFound     = errors.New("no rows returned")
)

func validateTableName(table string) error {
	if !validTables[table] {
		return errors.Wrapf(errInvalidTable, "table %s not found in schema", table)
	}
	return nil
}

func New(ctx context.Context, cfg *Config) (BQClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	bq, err := bigquery.NewClient(ctx, cfg.ProjectID, option.WithCredentialsFile(cfg.CredsPath))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c := &bqClient{
		cfg:    cfg,
		client: bq,
	}
	return c, nil
}

func (c *bqClient) execute(ctx context.Context, query string, params []bigquery.QueryParameter, needsResults bool) (*bigquery.RowIterator, error) {
	q := c.client.Query(query)
	q.Parameters = params

	if needsResults {
		return q.Read(ctx)
	}

	job, err := q.Run(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := status.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	return nil, nil
}

func (c *bqClient) Put(ctx context.Context, table string, data any) error {
	if err := validateTableName(table); err != nil {
		return err
	}

	tags, err := ctag.GetTags("bigquery", data)
	if err != nil {
		return errors.WithStack(err)
	}

	var fields []string
	var placeholders []string
	var params []bigquery.QueryParameter

	for _, tag := range tags {
		fields = append(fields, tag.Name)
		placeholders = append(placeholders, fmt.Sprintf("@%s", tag.Name))
		params = append(params, bigquery.QueryParameter{
			Name:  tag.Name,
			Value: tag.Field,
		})
	}

	query := fmt.Sprintf(`
        INSERT INTO %s.%s
        (%s)
        VALUES
        (%s)`,
		c.cfg.DatasetID,
		table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err = c.execute(ctx, query, params, false)
	return err
}

func (c *bqClient) StreamPut(ctx context.Context, table string, data any) error {
	if err := validateTableName(table); err != nil {
		return err
	}

	if err := c.inserter(table).Put(ctx, data); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *bqClient) StreamPutAll(ctx context.Context, inputs map[string][]any) error {
	if len(inputs) == 0 {
		return errors.New("inputs cannot be empty")
	}

	for table, data := range inputs {
		if err := validateTableName(table); err != nil {
			return err
		}

		if err := c.inserter(table).Put(ctx, data); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (c *bqClient) Query(ctx context.Context, query string, params []bigquery.QueryParameter) (*bigquery.RowIterator, error) {
	return c.execute(ctx, query, params, true)
}

func (c *bqClient) QueryRow(ctx context.Context, query string, params []bigquery.QueryParameter, dst any) error {
	it, err := c.execute(ctx, query, params, true)
	if err != nil {
		return err
	}

	if err := it.Next(dst); err != nil {
		if err == iterator.Done {
			return ErrNotFound
		}
		return errors.WithStack(err)
	}
	return nil
}

func (c *bqClient) Get(ctx context.Context, table string, id string, dst any) error {
	if err := validateTableName(table); err != nil {
		return err
	}

	query := fmt.Sprintf(`
        SELECT *
        FROM %s.%s
        WHERE id = @id
        LIMIT 1`,
		c.cfg.DatasetID,
		table,
	)

	params := []bigquery.QueryParameter{
		{Name: "id", Value: id},
	}

	return c.QueryRow(ctx, query, params, dst)
}

func (c *bqClient) Update(ctx context.Context, table string, id string, updates map[string]any) error {
	if err := validateTableName(table); err != nil {
		return err
	}

	setStatements := make([]string, 0, len(updates))
	params := []bigquery.QueryParameter{
		{Name: "id", Value: id},
	}

	for field, value := range updates {
		setStatements = append(setStatements, fmt.Sprintf("%s = @%s", field, field))
		params = append(params, bigquery.QueryParameter{
			Name:  field,
			Value: value,
		})
	}

	query := fmt.Sprintf(`
        UPDATE %s.%s 
        SET %s
        WHERE id = @id`,
		c.cfg.DatasetID,
		table,
		strings.Join(setStatements, ", "),
	)

	_, err := c.execute(ctx, query, params, false)
	return err
}

func (c *bqClient) Delete(ctx context.Context, table string, id string) error {
	if err := validateTableName(table); err != nil {
		return err
	}

	query := fmt.Sprintf(`
        DELETE FROM %s.%s 
        WHERE id = @id`,
		c.cfg.DatasetID,
		table,
	)

	params := []bigquery.QueryParameter{
		{Name: "id", Value: id},
	}

	_, err := c.execute(ctx, query, params, false)
	return err
}

func (c *bqClient) Close() error {
	if err := c.client.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *Config) Validate() error {
	if c == nil {
		return errors.New("database configuration required")
	}
	if c.ProjectID == "" {
		return errors.New("database project ID required")
	}
	if c.DatasetID == "" {
		return errors.New("database dataset ID required")
	}
	if c.CredsPath == "" {
		return errors.New("database creds path required")
	}
	return nil
}

func (c *bqClient) inserter(table string) *bigquery.Inserter {
	inserter := c.client.Dataset(c.cfg.DatasetID).Table(table).Inserter()
	inserter.SkipInvalidRows = false
	inserter.IgnoreUnknownValues = false
	return inserter
}
