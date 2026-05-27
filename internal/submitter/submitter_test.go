package submitter

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/chain"
	indexerv1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/indexer/v1"
	pricev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/price/v1"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

type fakePrice struct {
	mu     sync.Mutex
	out    *pricev1.AggregatedPrice
	err    error
	called int
}

func (f *fakePrice) GetPrice(_ context.Context, _ string) (*pricev1.AggregatedPrice, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called++
	return f.out, f.err
}

type fakeSigner struct {
	digest []byte
	sigs   [][]byte
	addr   common.Address
}

func (f *fakeSigner) BuildDigest(_ *big.Int, _ common.Hash, _, _ *big.Int, _ common.Address) ([]byte, error) {
	return f.digest, nil
}
func (f *fakeSigner) Sign(_ []byte) ([][]byte, error) { return f.sigs, nil }
func (f *fakeSigner) NewBroadcaster() (*bind.TransactOpts, error) {
	// A bare opts with a Signer that no-ops is enough for tests — the chain
	// fake never broadcasts a real tx.
	return &bind.TransactOpts{
		From: f.addr,
		Signer: func(_ common.Address, t *types.Transaction) (*types.Transaction, error) {
			return t, nil
		},
		Context: context.Background(),
	}, nil
}
func (f *fakeSigner) BroadcasterAddress() common.Address { return f.addr }

type fakeChain struct {
	mu sync.Mutex

	gas       chain.GasStrategy
	gasErr    error
	nonce     uint64
	nonceErr  error
	submitTx  common.Hash
	submitErr error
	replaceTx common.Hash
	replaceErr error

	// receiptByCall returns receipts in order; len < poll counts yield ErrTxNotMined.
	receiptByCall []receiptStep
	receiptCalls  int32

	replaceCalls int32
}

type receiptStep struct {
	r   *types.Receipt
	err error
}

func (c *fakeChain) SuggestGas(_ context.Context, _ int) (chain.GasStrategy, error) {
	return c.gas, c.gasErr
}
func (c *fakeChain) NonceAt(_ context.Context, _ common.Address) (uint64, error) {
	return c.nonce, c.nonceErr
}
func (c *fakeChain) SubmitFulfillment(_ context.Context, _ *bind.TransactOpts,
	_ common.Address, _, _, _ *big.Int, _ [][]byte, _ chain.GasStrategy) (common.Hash, error) {
	return c.submitTx, c.submitErr
}
func (c *fakeChain) ReplaceFulfillment(_ context.Context, _ *bind.TransactOpts,
	_ common.Address, _, _, _ *big.Int, _ [][]byte, _ chain.GasStrategy) (common.Hash, error) {
	atomic.AddInt32(&c.replaceCalls, 1)
	return c.replaceTx, c.replaceErr
}
func (c *fakeChain) TxReceipt(_ context.Context, _ common.Hash) (*types.Receipt, error) {
	idx := atomic.AddInt32(&c.receiptCalls, 1) - 1
	c.mu.Lock()
	defer c.mu.Unlock()
	if int(idx) >= len(c.receiptByCall) {
		return nil, chain.ErrTxNotMined
	}
	step := c.receiptByCall[idx]
	return step.r, step.err
}
func (c *fakeChain) LatestRoundData(_ context.Context, _ common.Address) (*big.Int, *big.Int, error) {
	return big.NewInt(0), big.NewInt(0), nil
}

type fakeRepo struct {
	mu          sync.Mutex
	submissions []*models.Submission
	updates     []*models.Submission
	pendingIns  int
	pendingDel  int
	insertErr   error
}

func (r *fakeRepo) InsertSubmission(_ context.Context, s *models.Submission) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.insertErr != nil {
		return 0, r.insertErr
	}
	s.ID = int64(len(r.submissions) + 1)
	// store a snapshot copy
	cp := *s
	r.submissions = append(r.submissions, &cp)
	return s.ID, nil
}
func (r *fakeRepo) UpdateSubmission(_ context.Context, s *models.Submission) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *s
	r.updates = append(r.updates, &cp)
	return nil
}
func (r *fakeRepo) InsertPendingTx(_ context.Context, _ int64, _ string, _ uint64, _ []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pendingIns++
	return nil
}
func (r *fakeRepo) DeletePendingTx(_ context.Context, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pendingDel++
	return nil
}
func (r *fakeRepo) snapshot() ([]*models.Submission, []*models.Submission, int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	subs := append([]*models.Submission(nil), r.submissions...)
	upd := append([]*models.Submission(nil), r.updates...)
	return subs, upd, r.pendingIns, r.pendingDel
}

// ---------------------------------------------------------------------------
// Test harness builder
// ---------------------------------------------------------------------------

const aggHex = "0x075be31662c2548c4e940d7e769c328a34dcb281"

func newHarness(t *testing.T, fc *fakeChain, fp *fakePrice, fr *fakeRepo) *Submitter {
	t.Helper()
	fs := &fakeSigner{
		digest: make([]byte, 32),
		sigs:   [][]byte{{0x01}, {0x02}, {0x03}},
		addr:   common.HexToAddress("0xb000000000000000000000000000000000000001"),
	}
	cfg := &config.SubmissionConfig{
		MaxRetries:        2,
		ReplaceAfterSec:   1,
		GasMultiplier:     1.1,
		ConfirmTimeoutSec: 30,
	}
	conv := &config.ConversionConfig{OnChainDecimals: 8}
	aggregators := map[string]string{"weth": aggHex}
	return New(fc, fp, fs, fr, cfg, conv, aggregators,
		WithPollInterval(50*time.Millisecond),
	)
}

func goodPrice() *pricev1.AggregatedPrice {
	return &pricev1.AggregatedPrice{
		AssetId:      "weth",
		MedianPrice:  3450.20,
		AggregatedAt: timestamppb.Now(),
	}
}

func priceRequestedEvent(t *testing.T, agg, reqID string) *indexerv1.Event {
	t.Helper()
	return &indexerv1.Event{
		Meta: &indexerv1.EventMeta{ContractAddress: agg, BlockNumber: 100},
		Kind: indexerv1.EventKind_EVENT_KIND_PRICE_REQUESTED,
		Payload: &indexerv1.Event_PriceRequested{
			PriceRequested: &indexerv1.PriceRequestedEvent{ReqId: reqID, AssetId: "0xweth"},
		},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHandleEvent_HappyPath_ConfirmedReceipt(t *testing.T) {
	fc := &fakeChain{
		gas:      chain.GasStrategy{GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2)},
		nonce:    7,
		submitTx: common.HexToHash("0xaa"),
		receiptByCall: []receiptStep{
			{r: &types.Receipt{Status: types.ReceiptStatusSuccessful, GasUsed: 100000}, err: nil},
		},
	}
	fp := &fakePrice{out: goodPrice()}
	fr := &fakeRepo{}
	sub := newHarness(t, fc, fp, fr)

	err := sub.HandleEvent(context.Background(), priceRequestedEvent(t, aggHex, "1"))
	if err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	sub.Wait()

	subs, upd, pIn, pDel := fr.snapshot()
	if len(subs) != 1 || subs[0].Status != models.SubmissionStatusPending {
		t.Fatalf("expected one pending insert, got %v", subs)
	}
	if subs[0].SubmittedPrice != "345020000000" {
		t.Fatalf("converted price %s, want 345020000000", subs[0].SubmittedPrice)
	}
	if len(upd) == 0 || upd[len(upd)-1].Status != models.SubmissionStatusConfirmed {
		t.Fatalf("expected confirmed update, got %v", upd)
	}
	if pIn != 1 || pDel != 1 {
		t.Fatalf("pending tx in=%d, del=%d", pIn, pDel)
	}
}

func TestHandleEvent_UnknownAggregator_SkippedNotError(t *testing.T) {
	fc := &fakeChain{}
	fp := &fakePrice{out: goodPrice()}
	fr := &fakeRepo{}
	sub := newHarness(t, fc, fp, fr)

	bad := priceRequestedEvent(t, "0x0000000000000000000000000000000000000000", "1")
	if err := sub.HandleEvent(context.Background(), bad); err != nil {
		t.Fatalf("expected nil (skip), got %v", err)
	}
	if fp.called != 0 {
		t.Fatalf("price should not have been queried, called=%d", fp.called)
	}
}

func TestHandleEvent_BroadcastFailure_PersistsFailedRow(t *testing.T) {
	fc := &fakeChain{
		gas:       chain.GasStrategy{GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2)},
		nonce:     7,
		submitErr: errors.New("rpc down"),
	}
	fp := &fakePrice{out: goodPrice()}
	fr := &fakeRepo{}
	sub := newHarness(t, fc, fp, fr)

	if err := sub.HandleEvent(context.Background(), priceRequestedEvent(t, aggHex, "9")); err == nil {
		t.Fatal("expected broadcast error to propagate")
	}
	subs, _, _, _ := fr.snapshot()
	if len(subs) != 1 || subs[0].Status != models.SubmissionStatusFailed {
		t.Fatalf("expected one failed insert, got %v", subs)
	}
	if subs[0].LastError == "" {
		t.Fatal("expected LastError populated on failure")
	}
}

func TestHandleHeartbeat_UsesReqIDZero(t *testing.T) {
	fc := &fakeChain{
		gas:      chain.GasStrategy{GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2)},
		nonce:    7,
		submitTx: common.HexToHash("0xbb"),
		receiptByCall: []receiptStep{
			{r: &types.Receipt{Status: types.ReceiptStatusSuccessful, GasUsed: 50000}, err: nil},
		},
	}
	fp := &fakePrice{out: goodPrice()}
	fr := &fakeRepo{}
	sub := newHarness(t, fc, fp, fr)

	if err := sub.HandleHeartbeat(context.Background(), "weth"); err != nil {
		t.Fatalf("HandleHeartbeat: %v", err)
	}
	sub.Wait()

	subs, _, _, _ := fr.snapshot()
	if len(subs) != 1 || subs[0].ReqID != models.HeartbeatReqID {
		t.Fatalf("expected reqId=0 (heartbeat), got %v", subs)
	}
}

func TestWatch_ReplaceByFeeOnTimeout(t *testing.T) {
	// First two poll calls return ErrTxNotMined; we expect a replace to fire.
	// Then a final poll returns success on the new tx.
	fc := &fakeChain{
		gas:       chain.GasStrategy{GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2)},
		nonce:     7,
		submitTx:  common.HexToHash("0xaa"),
		replaceTx: common.HexToHash("0xbb"),
		receiptByCall: []receiptStep{
			{nil, chain.ErrTxNotMined},
			{nil, chain.ErrTxNotMined},
			{r: &types.Receipt{Status: types.ReceiptStatusSuccessful}, err: nil},
		},
	}
	fp := &fakePrice{out: goodPrice()}
	fr := &fakeRepo{}
	sub := newHarness(t, fc, fp, fr)
	sub.cfg.ReplaceAfterSec = 0 // replace eagerly so the test stays fast

	if err := sub.HandleEvent(context.Background(), priceRequestedEvent(t, aggHex, "11")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	sub.Wait()

	if atomic.LoadInt32(&fc.replaceCalls) == 0 {
		t.Fatal("expected at least one replace-by-fee broadcast")
	}
	subs, upd, _, _ := fr.snapshot()
	if len(subs) != 1 || subs[0].ReqID != "11" {
		t.Fatalf("submissions: %v", subs)
	}
	// Last update should be confirmed (on the replacement tx hash).
	last := upd[len(upd)-1]
	if last.Status != models.SubmissionStatusConfirmed {
		t.Fatalf("last update status %v, want confirmed", last.Status)
	}
}

func TestWatch_DropsAfterMaxRetries(t *testing.T) {
	fc := &fakeChain{
		gas:       chain.GasStrategy{GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2)},
		nonce:     7,
		submitTx:  common.HexToHash("0xaa"),
		replaceTx: common.HexToHash("0xbb"),
		// No receipt ever — every poll returns ErrTxNotMined.
		receiptByCall: nil,
	}
	fp := &fakePrice{out: goodPrice()}
	fr := &fakeRepo{}
	sub := newHarness(t, fc, fp, fr)
	sub.cfg.ReplaceAfterSec = 0
	sub.cfg.MaxRetries = 1
	sub.cfg.ConfirmTimeoutSec = 60 // ensure we drop on retry, not on timeout

	if err := sub.HandleEvent(context.Background(), priceRequestedEvent(t, aggHex, "12")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	sub.Wait()

	_, upd, _, _ := fr.snapshot()
	if upd[len(upd)-1].Status != models.SubmissionStatusDropped {
		t.Fatalf("last update %v, want dropped", upd[len(upd)-1].Status)
	}
}

func TestHandleHeartbeat_UnknownSymbol(t *testing.T) {
	sub := newHarness(t, &fakeChain{}, &fakePrice{out: goodPrice()}, &fakeRepo{})
	if err := sub.HandleHeartbeat(context.Background(), "xau"); err == nil {
		t.Fatal("expected unknown-symbol error")
	}
}

func TestNew_BuildsAddressLookups(t *testing.T) {
	sub := New(&fakeChain{}, &fakePrice{}, &fakeSigner{}, &fakeRepo{},
		&config.SubmissionConfig{}, &config.ConversionConfig{OnChainDecimals: 8},
		map[string]string{
			"WETH": "0x075be31662c2548c4e940d7e769c328a34dcb281",
			"WBTC": "0xf8ad3a2505eece7ad276db038c7c56930bd436e4",
		},
	)
	if _, ok := sub.AggregatorBySymbol("weth"); !ok {
		t.Fatal("weth lookup failed")
	}
	if _, ok := sub.AggregatorBySymbol("Wbtc"); !ok {
		t.Fatal("case-insensitive WBTC lookup failed")
	}
	got := sub.SymbolsManaged()
	if len(got) != 2 {
		t.Fatalf("expected 2 symbols, got %v", got)
	}
}
