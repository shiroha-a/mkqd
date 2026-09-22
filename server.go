package mkqd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// healthServer serves the liveness, readiness and metrics endpoints.
//
// Routes:
//
//	GET /healthz  200 while the process is alive
//	GET /readyz   200 when workers are up and Redis answers PING, else 503
//	GET /metrics  Prometheus exposition (only when server.metrics is true)
type healthServer struct {
	srv *http.Server
	ln  net.Listener
}

func newHealthServer(rt *Runtime) (*healthServer, error) {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writePlain(w, http.StatusOK, "ok")
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !rt.isServing() {
			writePlain(w, http.StatusServiceUnavailable, "not serving")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), pingTimeout)
		defer cancel()
		if err := rt.ping(ctx); err != nil {
			writePlain(w, http.StatusServiceUnavailable, "redis unavailable: "+err.Error())
			return
		}
		writePlain(w, http.StatusOK, "ready")
	})

	if rt.cfg.Server.Metrics && rt.reg != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(rt.reg, promhttp.HandlerOpts{}))
	}

	ln, err := net.Listen("tcp", rt.cfg.Server.Addr)
	if err != nil {
		return nil, fmt.Errorf("mkqd: listen on %s: %w", rt.cfg.Server.Addr, err)
	}

	hs := &healthServer{
		ln: ln,
		srv: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
	go func() {
		if err := hs.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			rt.log.Error("health server stopped", "error", err)
		}
	}()
	return hs, nil
}

// Addr reports the address actually bound, which differs from the
// configured one when port 0 was requested (as tests do).
func (h *healthServer) Addr() string { return h.ln.Addr().String() }

// Close shuts the listener down, bounded by ctx.
func (h *healthServer) Close(ctx context.Context) error {
	err := h.srv.Shutdown(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return h.srv.Close()
	}
	return err
}

func writePlain(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body + "\n"))
}
