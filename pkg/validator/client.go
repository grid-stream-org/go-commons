package validator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	pb "github.com/grid-stream-org/grid-stream-protos/gen/validator/v1"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ValidatorClient interface {
	SendAverages(ctx context.Context, averages []*pb.AverageOutput) error
	Close() error
}

type Config struct {
	Host string `koanf:"host" json:"host" envconfig:"host"`
	Port int    `koanf:"port" json:"port" envconfig:"port"`
}

type validatorClient struct {
	cfg    *Config
	client pb.ValidatorServiceClient
	conn   *grpc.ClientConn
}

type ValidationErrors struct {
	NotValid bool
	Errors   []*pb.ValidationError
}

func (ve *ValidationErrors) Error() string {
	var messages []string
	for _, err := range ve.Errors {
		messages = append(messages, fmt.Sprintf("project %s: %s", err.ProjectId, err.Message))
	}
	return "validation failed: " + strings.Join(messages, "; ")
}

func (c *Config) Validate() error {
	if c.Port <= 0 {
		return errors.New("port must be greater than 0")
	}

	return nil
}

func New(ctx context.Context, cfg *Config, log *slog.Logger) (ValidatorClient, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c := &validatorClient{
		cfg:    cfg,
		client: pb.NewValidatorServiceClient(conn),
		conn:   conn,
	}

	log.Info("validator client created successfully", "serverAddress", addr)

	return c, nil
}

func (c *validatorClient) Close() error {
	return c.conn.Close()
}

func (c *validatorClient) SendAverages(ctx context.Context, averageOutputs []*pb.AverageOutput) error {
	req := &pb.ValidateAverageOutputsRequest{
		AverageOutputs: averageOutputs,
	}

	res, err := c.client.ValidateAverageOutputs(ctx, req)
	if err != nil {
		return errors.WithStack(err)
	}

	if !res.Success {
		return &ValidationErrors{
			NotValid: res.NotValid,
			Errors:   res.Errors,
		}
	}
	return nil
}
