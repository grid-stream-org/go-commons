package bqclient

import (
	"context"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
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
	tableDREvents        = "dr_events"
)

var validTables = map[string]bool{
	tableProjects:        true,
	tableContracts:       true,
	tableDERMetadata:     true,
	tableDERData:         true,
	tableProjectAverages: true,
	tableUtilities:       true,
	tableDREvents:        true,
}

type BQClient interface {
	Put(ctx context.Context, table string, data any) error
	StreamRead(ctx context.Context, table string, projectIDs []string) (<-chan []byte, <-chan error)
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
	cfg        *Config
	client     *bigquery.Client
	readClient *storage.BigQueryReadClient
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

	client, err := bigquery.NewClient(ctx, cfg.ProjectID, option.WithCredentialsFile(cfg.CredsPath))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	readClient, err := storage.NewBigQueryReadClient(ctx, option.WithCredentialsFile(cfg.CredsPath))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c := &bqClient{
		cfg:        cfg,
		client:     client,
		readClient: readClient,
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

func (c *bqClient) StreamRead(ctx context.Context, table string, projectIDs []string) (<-chan []byte, <-chan error) {
	dataChan := make(chan []byte, 100)
	errChan := make(chan error, 1)

	if err := validateTableName(table); err != nil {
		errChan <- err
		close(dataChan)
		close(errChan)
		return dataChan, errChan
	}

	// Create the project_id filter condition
	filter := ""
	if len(projectIDs) > 0 {
		quoted := make([]string, len(projectIDs))
		for i, id := range projectIDs {
			quoted[i] = fmt.Sprintf("'%s'", id)
		}
		filter = fmt.Sprintf("project_id IN (%s)", strings.Join(quoted, ","))
	}

	parent := fmt.Sprintf("projects/%s", c.cfg.ProjectID)
	tablePath := fmt.Sprintf("projects/%s/datasets/%s/tables/%s",
		c.cfg.ProjectID, c.cfg.DatasetID, table)

	session, err := c.readClient.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: parent,
		ReadSession: &storagepb.ReadSession{
			Table:      tablePath,
			DataFormat: storagepb.DataFormat_AVRO,
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				RowRestriction: filter, // Apply the filter here
			},
		},
		MaxStreamCount: 1,
	})

	// Rest of the function remains the same as your original StreamRead
	if err != nil {
		errChan <- err
		close(dataChan)
		close(errChan)
		return dataChan, errChan
	}

	if len(session.Streams) == 0 {
		errChan <- errors.New("no streams in session")
		close(dataChan)
		close(errChan)
		return dataChan, errChan
	}

	go func() {
		defer close(dataChan)
		defer close(errChan)
		streamReader, err := c.readClient.ReadRows(ctx, &storagepb.ReadRowsRequest{
			ReadStream: session.Streams[0].Name,
		})
		if err != nil {
			errChan <- err
			return
		}
		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				res, err := streamReader.Recv()
				if err == io.EOF {
					return
				}
				if err != nil {
					errChan <- err
					return
				}
				dataChan <- res.GetAvroRows().GetSerializedBinaryRows()
			}
		}
	}()
	return dataChan, errChan
}

func (c *bqClient) Close() error {
	if err := c.client.Close(); err != nil {
		return errors.WithStack(err)
	}

	if err := c.readClient.Close(); err != nil {
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
