// Package healthz hosts the /healthz, /readyz, /metrics HTTP listener on a
// dedicated port so the operational surface is decoupled from the gRPC
// transport.
package healthz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
)

// ReadinessProbe is the application-supplied closure that returns nil when
// the service is ready to accept traffic, or an error explaining why not.
type ReadinessProbe func(ctx context.Context) error

// Server wraps a stdlib http.Server with the three endpoints.
type Server struct {
	cfg       *config.HealthzConfig
	registry  *prometheus.Registry
	readiness ReadinessProbe
	log       *logrus.Entry

	srv *http.Server
	ln  net.Listener
}

// New constructs a Server. registry is the service-owned Prometheus registry
// from internal/metrics; readiness is the closure returning current health.
func New(cfg *config.HealthzConfig, registry *prometheus.Registry, readiness ReadinessProbe, log *logrus.Entry) *Server {
	if log == nil {
		log = logrus.NewEntry(logrus.StandardLogger()).WithField("component", "healthz")
	}
	return &Server{cfg: cfg, registry: registry, readiness: readiness, log: log}
}

// Serve binds the listener and starts the server. Returns once Stop() is
// called or the listener errors.
func (s *Server) Serve() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	ln, err := net.Listen("tcp", addr) //nolint:noctx // healthz listener has no context surface here
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	s.ln = ln

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))

	s.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	s.log.WithField("addr", addr).Info("healthz listener up")

	if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("healthz serve: %w", err)
	}
	return nil
}

// Stop drains the server.
func (s *Server) Stop(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

// JSON field key used across the three probe responses.
const statusKey = "status"

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		statusKey: "ok",
		"author":  "Andrei Solovov",
		"contact": "https://github.com/asolovov",
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.readiness == nil {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{statusKey: "ready"})
		return
	}
	if err := s.readiness(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			statusKey: "not-ready",
			"reason":  err.Error(),
		})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{statusKey: "ready"})
}
