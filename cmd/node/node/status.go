package node

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
)

func writeRemoteStatus(ctx context.Context, out io.Writer, cfg nodeconfig.Metrics, mode string) error {
	if !cfg.Enabled {
		return fmt.Errorf("status is unavailable: metrics.enabled is false")
	}

	host, port, err := net.SplitHostPort(strings.TrimSpace(cfg.ListenAddr))
	if err != nil {
		return fmt.Errorf("invalid metrics.listen_addr %q: %w", cfg.ListenAddr, err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber <= 0 || portNumber > 65535 {
		return fmt.Errorf("metrics.listen_addr must contain a fixed TCP port between 1 and 65535")
	}

	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	case "localhost":
	default:
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("local status requires metrics.listen_addr to bind a loopback or wildcard address")
		}
	}

	endpoint := url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: "/status"}
	if mode != "short" {
		endpoint.RawQuery = url.Values{"mode": {mode}}.Encode()
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("create status request: %w", err)
	}
	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("get status from %s: %w", endpoint.String(), err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read status response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("status endpoint returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}

	written, err := out.Write(body)
	if err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if written != len(body) {
		return fmt.Errorf("write status: %w", io.ErrShortWrite)
	}

	return nil
}
