// Package submitter is the oracle-service's price-submission business logic.
//
// Entry points:
//
//   - HandleEvent(ctx, ev): called by the stream consumer for each
//     PriceRequestedEvent delivered past the indexer confirmation gate.
//     Fetches the aggregated price from price-service, converts to the
//     on-chain int256 scale, signs the EIP-712 digest with each reporter
//     key, broadcasts fulfillPrice, persists the submission row, and kicks
//     off a background watcher that drives the tx to terminal state
//     (confirmed | failed | dropped).
//
//   - HandleHeartbeat(ctx, assetID): called by the heartbeat scheduler for
//     each per-asset tick. Heartbeat submissions use reqId == 0 per spec.
//
// Architecture rule 4 / 5: this is internal business logic, not a template
// module. application.go wires Submitter once and hands the dispatcher seam
// to the stream consumer + heartbeat scheduler.
package submitter

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/chain"
	indexerv1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/indexer/v1"
	pricev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/price/v1"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
)

// ChainClient is the chain-package surface the submitter depends on. Defined
// as an interface so unit tests can sub a fake without a live RPC.
type ChainClient interface {
	SuggestGas(ctx context.Context, attempt int) (chain.GasStrategy, error)
	NonceAt(ctx context.Context, addr common.Address) (uint64, error)
	SubmitFulfillment(ctx context.Context, auth *bind.TransactOpts,
		aggregator common.Address, reqID, price, timestamp *big.Int,
		signatures [][]byte, gas chain.GasStrategy) (common.Hash, error)
	ReplaceFulfillment(ctx context.Context, auth *bind.TransactOpts,
		aggregator common.Address, reqID, price, timestamp *big.Int,
		signatures [][]byte, gas chain.GasStrategy) (common.Hash, error)
	TxReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)
	LatestRoundData(ctx context.Context, aggregator common.Address) (*big.Int, *big.Int, error)
}

// PriceClient is the price-service gRPC surface.
type PriceClient interface {
	GetPrice(ctx context.Context, assetSymbol string) (*pricev1.AggregatedPrice, error)
}

// SignerClient is the signer.Signer surface.
type SignerClient interface {
	BuildDigest(reqID *big.Int, assetID common.Hash, price, timestamp *big.Int, aggregator common.Address) ([]byte, error)
	Sign(digest []byte) ([][]byte, error)
	NewBroadcaster() (*bind.TransactOpts, error)
	BroadcasterAddress() common.Address
}

// Repo is the repository surface the submitter needs.
type Repo interface {
	InsertSubmission(ctx context.Context, s *models.Submission) (int64, error)
	UpdateSubmission(ctx context.Context, s *models.Submission) error
	InsertPendingTx(ctx context.Context, submissionID int64, txHash string, nonce uint64, gasStrategyJSON []byte) error
	DeletePendingTx(ctx context.Context, txHash string) error
}

// Submitter is the wiring point. One per service.
type Submitter struct {
	chain  ChainClient
	price  PriceClient
	signer SignerClient
	repo   Repo
	cfg    *config.SubmissionConfig
	conv   *config.ConversionConfig

	// Address->symbol reverse lookup, built once from config.Chain.AggregatorAddresses.
	bySymbol  map[string]common.Address
	byAddress map[common.Address]string

	log *logrus.Entry

	wg sync.WaitGroup

	// Polling cadence for the per-submission watcher. Defaults to 5s; tests
	// override via WithPollInterval to keep them fast.
	pollEvery time.Duration

	// Metrics hooks (nil-tolerant).
	onSubmission func(asset, status string)
	onGasUsed    func(uint64)
}

// Option tunes the Submitter at construction time.
type Option func(*Submitter)

// WithLogger sets the structured-log handle.
func WithLogger(log *logrus.Entry) Option {
	return func(s *Submitter) { s.log = log }
}

// WithMetricsHooks wires Prometheus counters. Nil hooks are ignored.
func WithMetricsHooks(onSubmission func(asset, status string), onGasUsed func(uint64)) Option {
	return func(s *Submitter) {
		s.onSubmission = onSubmission
		s.onGasUsed = onGasUsed
	}
}

// WithPollInterval overrides the receipt-poll cadence. Used by tests.
func WithPollInterval(d time.Duration) Option {
	return func(s *Submitter) { s.pollEvery = d }
}

// New constructs a Submitter from its dependencies + the chain aggregator
// table from config.
func New(c ChainClient, p PriceClient, sgn SignerClient, repo Repo,
	cfg *config.SubmissionConfig, conv *config.ConversionConfig,
	aggregatorAddresses map[string]string, opts ...Option,
) *Submitter {
	bySym := make(map[string]common.Address, len(aggregatorAddresses))
	byAddr := make(map[common.Address]string, len(aggregatorAddresses))
	for sym, addr := range aggregatorAddresses {
		s := strings.ToLower(sym)
		a := common.HexToAddress(addr)
		bySym[s] = a
		byAddr[a] = s
	}

	sub := &Submitter{
		chain: c, price: p, signer: sgn, repo: repo,
		cfg: cfg, conv: conv,
		bySymbol: bySym, byAddress: byAddr,
		pollEvery: 5 * time.Second,
		log:       logrus.NewEntry(logrus.StandardLogger()).WithField("component", "submitter"),
	}
	for _, opt := range opts {
		opt(sub)
	}
	return sub
}

// Wait blocks until all in-flight tx watchers exit. Called by Stop().
func (s *Submitter) Wait() { s.wg.Wait() }

// AggregatorBySymbol returns the configured aggregator for a lowercase
// symbol. Used by the heartbeat scheduler.
func (s *Submitter) AggregatorBySymbol(symbol string) (common.Address, bool) {
	a, ok := s.bySymbol[strings.ToLower(symbol)]
	return a, ok
}

// SymbolsManaged returns the lowercase asset symbols this service manages.
func (s *Submitter) SymbolsManaged() []string {
	out := make([]string, 0, len(s.bySymbol))
	for sym := range s.bySymbol {
		out = append(out, sym)
	}
	return out
}

// HandleEvent is the streamconsumer.Dispatcher seam.
func (s *Submitter) HandleEvent(ctx context.Context, ev *indexerv1.Event) error {
	pr := ev.GetPriceRequested()
	if pr == nil {
		return errors.New("event is not PriceRequested")
	}
	aggregatorAddr := common.HexToAddress(ev.GetMeta().GetContractAddress())

	symbol, ok := s.byAddress[aggregatorAddr]
	if !ok {
		s.log.WithField("aggregator", aggregatorAddr.Hex()).
			Warn("skipping event from unknown aggregator")
		// Returning nil so the stream consumer still advances its cursor; the
		// alternative would re-receive forever events for assets we don't
		// manage.
		return nil
	}

	reqID, ok := models.ReqIDToBigInt(pr.GetReqId())
	if !ok {
		return fmt.Errorf("malformed req_id %q", pr.GetReqId())
	}

	return s.submit(ctx, symbol, aggregatorAddr, reqID)
}

// HandleHeartbeat is the heartbeat-scheduler seam.
func (s *Submitter) HandleHeartbeat(ctx context.Context, symbol string) error {
	aggregatorAddr, ok := s.AggregatorBySymbol(symbol)
	if !ok {
		return fmt.Errorf("unknown symbol %q", symbol)
	}
	return s.submit(ctx, symbol, aggregatorAddr, big.NewInt(0))
}

// submit is the shared inner path. The only difference between consumer-
// driven and heartbeat submissions is the reqId.
func (s *Submitter) submit(ctx context.Context, symbol string, aggregator common.Address, reqID *big.Int) error {
	log := s.log.WithFields(logrus.Fields{
		"symbol":     symbol,
		"req_id":     reqID.String(),
		"aggregator": aggregator.Hex(),
	})

	priceMsg, err := s.price.GetPrice(ctx, symbol)
	if err != nil {
		return fmt.Errorf("get price for %s: %w", symbol, err)
	}

	priceInt, err := models.FloatToInt256(priceMsg.GetMedianPrice(), s.conv.OnChainDecimals)
	if err != nil {
		return fmt.Errorf("convert price to int256: %w", err)
	}

	assetID, err := models.NewAssetIDFromSymbol(symbol)
	if err != nil {
		return fmt.Errorf("derive on-chain asset id: %w", err)
	}

	ts := time.Now().UTC()
	if msgTs := priceMsg.GetAggregatedAt(); msgTs != nil {
		ts = msgTs.AsTime()
	}
	tsBI := big.NewInt(ts.Unix())

	digest, err := s.signer.BuildDigest(reqID, assetID.OnChain, priceInt, tsBI, aggregator)
	if err != nil {
		return fmt.Errorf("build digest: %w", err)
	}
	sigs, err := s.signer.Sign(digest)
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}

	broadcaster := s.signer.BroadcasterAddress()
	nonce, err := s.chain.NonceAt(ctx, broadcaster)
	if err != nil {
		return fmt.Errorf("pin nonce: %w", err)
	}
	auth, err := s.signer.NewBroadcaster()
	if err != nil {
		return fmt.Errorf("build transactor opts: %w", err)
	}
	auth.Nonce = new(big.Int).SetUint64(nonce)

	gas, err := s.chain.SuggestGas(ctx, 0)
	if err != nil {
		return fmt.Errorf("suggest gas: %w", err)
	}

	txHash, err := s.chain.SubmitFulfillment(ctx, auth, aggregator, reqID, priceInt, tsBI, sigs, gas)
	if err != nil {
		// Broadcast-time failures (insufficient funds, RPC unreachable, nonce
		// race) are typically TRANSIENT — they don't represent a permanent
		// on-chain decision. Don't persist a FAILED row here; if we did, the
		// streamconsumer's ExistsByReqID idempotency check would latch this
		// req_id as terminal and skip it forever, even after the operator
		// fixes the underlying problem (e.g. funds the broadcaster wallet).
		//
		// Surfacing the error to the consumer also blocks cursor advance,
		// so the same event will be redelivered on the next stream pass
		// and we'll retry with no state to clean up.
		//
		// Reverted-on-chain failures (the watcher's path, after a tx hash
		// exists) DO persist FAILED — those are deterministic outcomes the
		// re-dispatch can't change.
		log.WithError(err).Error("broadcast fulfillPrice failed; not persisting (transient — will retry on next stream pass)")
		return fmt.Errorf("broadcast fulfillPrice: %w", err)
	}

	sub := &models.Submission{
		ReqID:          reqID.String(),
		AssetID:        symbol,
		Aggregator:     aggregator,
		TxHash:         txHash,
		SubmittedPrice: priceInt.String(),
		SubmittedAt:    time.Now().UTC(),
		Status:         models.SubmissionStatusPending,
	}
	id, err := s.repo.InsertSubmission(ctx, sub)
	if err != nil {
		return fmt.Errorf("persist submission: %w", err)
	}
	sub.ID = id
	if err := s.repo.InsertPendingTx(ctx, id, txHash.Hex(), nonce, nil); err != nil {
		log.WithError(err).Warn("persist pending tx (non-fatal)")
	}
	s.markSubmissionMetric(symbol, models.SubmissionStatusPending)
	log.WithField("tx_hash", txHash.Hex()).Info("fulfillPrice broadcast")

	s.wg.Add(1)
	//nolint:gosec // G118 — watcher must outlive the inbound request context so in-flight txs aren't stranded
	go func() {
		defer s.wg.Done()
		s.watch(context.Background(), sub, auth, aggregator, priceInt, tsBI, sigs)
	}()
	return nil
}

func (s *Submitter) watch(
	ctx context.Context,
	sub *models.Submission,
	auth *bind.TransactOpts,
	aggregator common.Address,
	priceInt, tsBI *big.Int,
	sigs [][]byte,
) {
	replaceAfter := time.Duration(s.cfg.ReplaceAfterSec) * time.Second
	confirmDeadline := time.Now().Add(time.Duration(s.cfg.ConfirmTimeoutSec) * time.Second)

	log := s.log.WithFields(logrus.Fields{
		"submission_id": sub.ID,
		"req_id":        sub.ReqID,
		"asset":         sub.AssetID,
	})

	lastBroadcast := time.Now()

	for {
		if time.Now().After(confirmDeadline) {
			s.markDropped(ctx, sub)
			log.Warn("submission dropped after confirm timeout")
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(s.pollEvery):
		}

		receipt, err := s.chain.TxReceipt(ctx, sub.TxHash)
		switch {
		case err == nil:
			s.finalizeFromReceipt(ctx, sub, receipt)
			if s.onGasUsed != nil {
				s.onGasUsed(receipt.GasUsed)
			}
			return
		case errors.Is(err, chain.ErrTxNotMined):
			// expected pre-mine state
		default:
			log.WithError(err).Warn("receipt poll error (will retry)")
			continue
		}

		if time.Since(lastBroadcast) < replaceAfter {
			continue
		}
		if sub.RetryCount >= s.cfg.MaxRetries {
			s.markDropped(ctx, sub)
			log.Warn("submission dropped after max retries")
			return
		}

		newGas, err := s.chain.SuggestGas(ctx, sub.RetryCount+1)
		if err != nil {
			log.WithError(err).Warn("replace gas suggest failed")
			continue
		}
		newHash, err := s.chain.ReplaceFulfillment(ctx, auth, aggregator,
			mustBigInt(sub.ReqID), priceInt, tsBI, sigs, newGas)
		if err != nil {
			log.WithError(err).Warn("replace broadcast failed")
			continue
		}
		old := sub.TxHash
		sub.TxHash = newHash
		sub.RetryCount++
		sub.LastError = ""
		if uerr := s.repo.UpdateSubmission(ctx, sub); uerr != nil {
			log.WithError(uerr).Warn("update submission after replace")
		}
		_ = s.repo.DeletePendingTx(ctx, old.Hex())
		_ = s.repo.InsertPendingTx(ctx, sub.ID, newHash.Hex(), auth.Nonce.Uint64(), nil)
		lastBroadcast = time.Now()
		log.WithFields(logrus.Fields{
			"old_tx": old.Hex(),
			"new_tx": newHash.Hex(),
			"retry":  sub.RetryCount,
		}).Info("replace-by-fee submitted")
	}
}

func (s *Submitter) finalizeFromReceipt(ctx context.Context, sub *models.Submission, r *types.Receipt) {
	switch r.Status {
	case types.ReceiptStatusSuccessful:
		sub.Status = models.SubmissionStatusConfirmed
	default:
		sub.Status = models.SubmissionStatusFailed
		sub.LastError = "tx reverted"
	}
	if err := s.repo.UpdateSubmission(ctx, sub); err != nil {
		s.log.WithError(err).Warn("update submission on finalize")
	}
	_ = s.repo.DeletePendingTx(ctx, sub.TxHash.Hex())
	s.markSubmissionMetric(sub.AssetID, sub.Status)
}

func (s *Submitter) markDropped(ctx context.Context, sub *models.Submission) {
	sub.Status = models.SubmissionStatusDropped
	if err := s.repo.UpdateSubmission(ctx, sub); err != nil {
		s.log.WithError(err).Warn("update submission on dropped")
	}
	_ = s.repo.DeletePendingTx(ctx, sub.TxHash.Hex())
	s.markSubmissionMetric(sub.AssetID, sub.Status)
}

func (s *Submitter) markSubmissionMetric(asset string, st models.SubmissionStatus) {
	if s.onSubmission != nil {
		s.onSubmission(asset, st.String())
	}
}

func mustBigInt(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return big.NewInt(0)
	}
	return v
}
