package grpcclient

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	indexerv1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/indexer/v1"
)

// IndexerClient holds the gRPC connection to indexer-service. The
// generated indexerv1.IndexerServiceClient already satisfies
// streamconsumer.StreamClient — no extra wrapping needed.
type IndexerClient struct {
	conn   *grpc.ClientConn
	client indexerv1.IndexerServiceClient
}

// DialIndexer opens a connection to indexer-service.
func DialIndexer(ctx context.Context, cfg *config.IndexerClientConfig) (*IndexerClient, error) {
	if cfg == nil || cfg.Address == "" {
		return nil, errors.New("indexer client config missing address")
	}
	cc, err := grpc.NewClient(cfg.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial indexer service: %w", err)
	}
	cc.Connect()
	return &IndexerClient{conn: cc, client: indexerv1.NewIndexerServiceClient(cc)}, nil
}

// StreamClient returns the underlying generated client; satisfies
// streamconsumer.StreamClient.
func (c *IndexerClient) StreamClient() indexerv1.IndexerServiceClient {
	return c.client
}

// Close releases the connection.
func (c *IndexerClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
