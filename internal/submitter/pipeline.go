package submitter

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"

	chainpkg "github.com/asolovov/evm-oracle-demo-oracle-service/internal/chain"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
)

// maxBackoff caps the per-request retry backoff so a persistently un-priceable
// asset retries at a steady ~30s cadence (occupying one worker slot briefly per
// attempt) until its TTL expires.
const maxBackoff = 30 * time.Second

// ---------------------------------------------------------------------------
// Worker pool — price + convert + clamp + sign (pre-broadcast, concurrent)
// ---------------------------------------------------------------------------

func (s *Submitter) runWorker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		case item := <-s.requests:
			s.process(item)
		}
	}
}

// process turns a queued request into a signed payload and hands it to the
// sender. Every failure path is per-request: it never blocks other assets.
func (s *Submitter) process(item *workItem) {
	ctx := context.Background()
	log := s.itemLog(item)

	if s.itemExpired(item) {
		s.expire(item, "ttl exceeded before processing")
		return
	}

	start := time.Now()

	priceMsg, err := s.price.GetPrice(ctx, item.symbol)
	if err != nil {
		// Transient (price-service down, or NotFound "no price yet"): retry
		// within TTL. This is the case that USED to wedge the whole stream.
		s.retryOrExpire(item, "get price: "+err.Error())
		return
	}

	priceInt, err := models.FloatToInt256(priceMsg.GetMedianPrice(), s.conv.OnChainDecimals)
	if err != nil {
		s.failPreBroadcast(item, "", "convert price to int256: "+err.Error())
		return
	}

	assetID, ok := s.assetIDByAggregator[item.aggregator]
	if !ok {
		s.failPreBroadcast(item, priceInt.String(), "missing on-chain assetId for aggregator "+item.aggregator.Hex())
		return
	}

	// Timestamp clamp: prefer the price's aggregated_at, but stay strictly
	// above the previous round's startedAt or the contract reverts with
	// StaleTimestamp (monotonic guard).
	ts := time.Now().UTC()
	if msgTs := priceMsg.GetAggregatedAt(); msgTs != nil {
		ts = msgTs.AsTime()
	}
	tsBI := big.NewInt(ts.Unix())
	latestStartedAt, err := s.chain.LatestStartedAt(ctx, item.aggregator)
	if err != nil {
		s.retryOrExpire(item, "read latestStartedAt: "+err.Error())
		return
	}
	if floor := new(big.Int).Add(latestStartedAt, big.NewInt(1)); tsBI.Cmp(floor) < 0 {
		tsBI = floor
	}

	digest, err := s.signer.BuildDigest(item.reqID, assetID, priceInt, tsBI, item.aggregator)
	if err != nil {
		s.failPreBroadcast(item, priceInt.String(), "build digest: "+err.Error())
		return
	}
	sigs, err := s.signer.Sign(digest)
	if err != nil {
		s.failPreBroadcast(item, priceInt.String(), "sign: "+err.Error())
		return
	}

	if s.onProcessing != nil {
		s.onProcessing(time.Since(start).Seconds())
	}
	s.markStatus(ctx, item, models.SubmissionStatusSending, priceInt.String(), "")

	select {
	case s.sendCh <- &signedTx{item: item, price: priceInt, ts: tsBI, sigs: sigs}:
	case <-s.stop:
	}
	_ = log
}

// retryOrExpire re-queues a request after backoff while within its TTL, else
// expires it. Heartbeats are dropped (the scheduler re-fires).
func (s *Submitter) retryOrExpire(item *workItem, cause string) {
	log := s.itemLog(item)
	if item.heartbeat {
		log.WithField("cause", cause).Warn("heartbeat price unavailable; dropping (scheduler will re-tick)")
		return
	}
	if s.itemExpired(item) {
		s.expire(item, cause)
		return
	}
	item.attempts++
	backoff := s.backoff(item.attempts)
	s.markStatus(context.Background(), item, models.SubmissionStatusQueued, "", cause)
	log.WithFields(logrus.Fields{"attempt": item.attempts, "backoff": backoff.String(), "cause": cause}).
		Warn("request retry scheduled (no other asset blocked)")
	s.scheduleRequeue(item, backoff)
}

// scheduleRequeue re-pushes the item onto the worker channel after backoff,
// re-checking the TTL at fire time.
func (s *Submitter) scheduleRequeue(item *workItem, backoff time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-s.stop:
			return
		case <-time.After(backoff):
		}
		if s.itemExpired(item) {
			s.expire(item, "ttl exceeded during backoff")
			return
		}
		select {
		case s.requests <- item:
		case <-s.stop:
		}
	}()
}

// expire terminally abandons a pre-broadcast request that blew its TTL.
func (s *Submitter) expire(item *workItem, reason string) {
	if item.submissionID != 0 {
		if err := s.repo.MarkExpired(context.Background(), item.submissionID, reason); err != nil {
			s.itemLog(item).WithError(err).Warn("mark expired")
		}
	}
	if s.onExpired != nil {
		s.onExpired(item.symbol)
	}
	s.itemLog(item).WithField("reason", reason).Warn("request expired (TTL); abandoning")
}

// failTerminal marks a request terminally failed for a deterministic,
// no-nonce-consumed failure (pre-broadcast conversion/sign/config error, or a
// broadcast-time revert). msg distinguishes the two in logs.
func (s *Submitter) failTerminal(item *workItem, price, cause, msg string) {
	ctx := context.Background()
	if item.heartbeat {
		_, _ = s.repo.InsertSubmission(ctx, &models.Submission{
			ReqID: models.HeartbeatReqID, AssetID: item.symbol, Aggregator: item.aggregator,
			SubmittedPrice: price, Status: models.SubmissionStatusFailed, LastError: cause,
			SubmittedAt: time.Now().UTC(),
		})
	} else if item.submissionID != 0 {
		s.markStatus(ctx, item, models.SubmissionStatusFailed, price, cause)
	}
	s.markSubmissionMetric(item.symbol, models.SubmissionStatusFailed)
	s.itemLog(item).WithField("cause", cause).Error(msg)
}

// failPreBroadcast marks a request terminally failed for a deterministic
// pre-broadcast error (conversion / sign / config), no nonce consumed.
func (s *Submitter) failPreBroadcast(item *workItem, price, cause string) {
	s.failTerminal(item, price, cause, "request failed pre-broadcast (permanent)")
}

// markStatus updates a queued row's status (+ optional price/error) by id.
// No-op for heartbeats (no durable row).
func (s *Submitter) markStatus(ctx context.Context, item *workItem, st models.SubmissionStatus, price, lastErr string) {
	if item.submissionID == 0 {
		return
	}
	sub := &models.Submission{
		ID:             item.submissionID,
		ReqID:          item.reqID.String(),
		AssetID:        item.symbol,
		Aggregator:     item.aggregator,
		SubmittedPrice: price,
		Status:         st,
		LastError:      lastErr,
		SubmittedAt:    time.Now().UTC(),
	}
	if err := s.repo.UpdateSubmission(ctx, sub); err != nil {
		s.itemLog(item).WithError(err).Warn("update status")
	}
}

// ---------------------------------------------------------------------------
// Sender — the single serialized stage; owns the broadcaster nonce counter
// ---------------------------------------------------------------------------

func (s *Submitter) runSender() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		case st := <-s.sendCh:
			s.send(st)
		}
	}
}

func (s *Submitter) send(st *signedTx) {
	ctx := context.Background()
	item := st.item
	log := s.itemLog(item)

	auth, err := s.signer.NewBroadcaster()
	if err != nil {
		s.retryOrExpire(item, "build transactor opts: "+err.Error())
		return
	}
	auth.Nonce = new(big.Int).SetUint64(s.nonce)

	gas, err := s.chain.SuggestGas(ctx, 0)
	if err != nil {
		// Transient; nonce NOT consumed.
		s.retryOrExpire(item, "suggest gas: "+err.Error())
		return
	}

	txHash, err := s.chain.SubmitFulfillment(ctx, auth, item.aggregator, item.reqID, st.price, st.ts, st.sigs, gas)
	if err != nil {
		if chainpkg.IsRevertError(err) {
			// Permanent — estimate/sim reverted; nonce NOT consumed.
			s.senderFail(item, st.price.String(), "broadcast reverted: "+err.Error())
			return
		}
		// Transient (RPC/funds/nonce race); nonce NOT consumed → reused next.
		s.retryOrExpire(item, "broadcast fulfillPrice: "+err.Error())
		return
	}

	// Success — nonce consumed; advance the counter (single-goroutine, no lock).
	nonceUsed := s.nonce
	s.nonce++

	sub := s.persistPending(ctx, item, st.price, txHash, nonceUsed)
	s.markSubmissionMetric(item.symbol, models.SubmissionStatusPending)
	log.WithFields(logrus.Fields{"tx_hash": txHash.Hex(), "nonce": nonceUsed}).Info("fulfillPrice broadcast")

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// Fresh context: the watcher must outlive the send call so an
		// in-flight tx isn't stranded; it exits on s.stop.
		s.watch(context.Background(), sub, auth, item.aggregator, st.price, st.ts, st.sigs)
	}()
}

// senderFail marks a request terminally failed for a permanent on-chain revert
// at broadcast time. No nonce was consumed.
func (s *Submitter) senderFail(item *workItem, price, cause string) {
	s.failTerminal(item, price, cause, "broadcast reverted; marked FAILED (permanent)")
}

// persistPending transitions a request to `pending` after a successful
// broadcast: updates the queued row (consumer-driven) or inserts a fresh row
// (heartbeat). Returns the submission with its id set, for the watcher.
func (s *Submitter) persistPending(ctx context.Context, item *workItem, price *big.Int, txHash common.Hash, nonce uint64) *models.Submission {
	sub := &models.Submission{
		ID:             item.submissionID,
		ReqID:          item.reqID.String(),
		AssetID:        item.symbol,
		Aggregator:     item.aggregator,
		TxHash:         txHash,
		SubmittedPrice: price.String(),
		SubmittedAt:    time.Now().UTC(),
		Status:         models.SubmissionStatusPending,
	}
	if item.submissionID != 0 {
		if err := s.repo.UpdateSubmission(ctx, sub); err != nil {
			s.itemLog(item).WithError(err).Warn("update submission to pending")
		}
	} else {
		id, err := s.repo.InsertSubmission(ctx, sub)
		if err != nil {
			s.itemLog(item).WithError(err).Error("persist heartbeat pending row")
		} else {
			sub.ID = id
		}
	}
	if err := s.repo.InsertPendingTx(ctx, sub.ID, txHash.Hex(), nonce, nil); err != nil {
		s.itemLog(item).WithError(err).Warn("persist pending tx (non-fatal)")
	}
	return sub
}

// ---------------------------------------------------------------------------
// Confirmation watcher (one goroutine per broadcast tx; no ordering constraint)
// ---------------------------------------------------------------------------

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

	log := s.log.WithFields(logrus.Fields{"submission_id": sub.ID, "req_id": sub.ReqID, "asset": sub.AssetID})
	lastBroadcast := time.Now()

	for {
		if time.Now().After(confirmDeadline) {
			s.markDropped(ctx, sub)
			log.Warn("submission dropped after confirm timeout")
			return
		}

		select {
		case <-s.stop:
			return
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
		case errors.Is(err, chainpkg.ErrTxNotMined):
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
		log.WithFields(logrus.Fields{"old_tx": old.Hex(), "new_tx": newHash.Hex(), "retry": sub.RetryCount}).
			Info("replace-by-fee submitted")
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

// ---------------------------------------------------------------------------
// Startup recovery
// ---------------------------------------------------------------------------

// recover re-enqueues durable pre-broadcast rows after a restart, and expires
// any whose TTL already passed. Rows already broadcast (`pending`) are owned
// by their watcher and not reloaded here (v1 limitation: a crash mid-watch
// leaves a pending row un-reconciled — documented).
func (s *Submitter) recover(ctx context.Context) {
	if n, err := s.repo.ExpireOverdue(ctx); err != nil {
		s.log.WithError(err).Warn("expire overdue on startup")
	} else if n > 0 {
		s.log.WithField("count", n).Info("expired overdue requests on startup")
	}

	rows, err := s.repo.LoadResumable(ctx)
	if err != nil {
		s.log.WithError(err).Warn("load resumable on startup")
		return
	}
	resumed := 0
	for _, sub := range rows {
		reqID, ok := models.ReqIDToBigInt(sub.ReqID)
		if !ok {
			continue
		}
		item := &workItem{
			submissionID: sub.ID,
			symbol:       sub.AssetID,
			aggregator:   sub.Aggregator,
			reqID:        reqID,
			expiresAt:    sub.ExpiresAt,
		}
		if s.itemExpired(item) {
			s.expire(item, "ttl exceeded (recovery)")
			continue
		}
		s.enqueueItem(item)
		resumed++
	}
	if resumed > 0 {
		s.log.WithField("count", resumed).Info("recovered resumable requests on startup")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *Submitter) itemExpired(item *workItem) bool {
	return !item.heartbeat && !item.expiresAt.IsZero() && time.Now().After(item.expiresAt)
}

func (s *Submitter) backoff(attempts int) time.Duration {
	base := s.retryBackoff
	if base <= 0 {
		base = 2 * time.Second
	}
	d := time.Duration(attempts) * base
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

func (s *Submitter) itemLog(item *workItem) *logrus.Entry {
	return s.log.WithFields(logrus.Fields{
		"symbol":     item.symbol,
		"req_id":     item.reqID.String(),
		"aggregator": item.aggregator.Hex(),
		"heartbeat":  item.heartbeat,
	})
}
