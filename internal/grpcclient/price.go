// Package grpcclient holds thin gRPC client wrappers for the external
// services the oracle-service consumes (price-service + indexer-service).
//
// Plain Go packages (rule 5 — external service handlers are NOT template
// modules). The wrappers exist so the submitter / heartbeat scheduler /
// stream consumer take a small, domain-shaped interface rather than the
// full generated client.
package grpcclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	pricev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/price/v1"
)

// PriceClient wraps pricev1.PriceServiceClient with a domain-shaped GetPrice
// signature (symbol -> *AggregatedPrice) the submitter expects.
type PriceClient struct {
	conn    *grpc.ClientConn
	client  pricev1.PriceServiceClient
	timeout time.Duration
}

// DialPrice opens a connection to price-service. Caller owns Close().
func DialPrice(ctx context.Context, cfg *config.PriceClientConfig) (*PriceClient, error) {
	if cfg == nil || cfg.Address == "" {
		return nil, errors.New("price client config missing address")
	}
	cc, err := grpc.NewClient(cfg.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial price service: %w", err)
	}
	// Eager state check is best-effort — the streaming surface establishes
	// the real connection lazily on first call.
	cc.Connect()

	return &PriceClient{
		conn:    cc,
		client:  pricev1.NewPriceServiceClient(cc),
		timeout: time.Duration(cfg.TimeoutSec) * time.Second,
	}, nil
}

// GetPrice issues a unary GetPrice call with the configured per-RPC deadline.
func (c *PriceClient) GetPrice(ctx context.Context, symbol string) (*pricev1.AggregatedPrice, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	return c.client.GetPrice(ctx, &pricev1.GetPriceRequest{AssetId: symbol})
}

// Close releases the underlying connection.
func (c *PriceClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
