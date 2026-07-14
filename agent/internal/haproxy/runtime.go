// Package haproxy implements the HAProxy runtime API client over a unix
// stats socket. The runtime socket serves exactly one command per connection
// in non-interactive mode, so every Exec dials a fresh connection.
package haproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// showStatCmd requests proxy type mask 1 (frontends only) for all proxies
// and all servers: "show stat <proxy_id> <type> <server_id>".
const showStatCmd = "show stat -1 1 -1"

// errorReplyPrefixes mark replies that unambiguously indicate a failed
// command. Anything else is returned verbatim for the caller to interpret.
var errorReplyPrefixes = []string{
	"Unknown command",
	"Permission denied",
	"[", // e.g. "[ALERT]", "[CFGERR]" style bracketed diagnostics
}

// Client talks to one HAProxy process via its stats socket. It satisfies
// model.StatSource and model.MapSetter.
type Client struct {
	socketPath string
	timeout    time.Duration // per-command budget covering dial + write + read
}

// New returns a client for the runtime socket at socketPath. timeout bounds
// each command end to end; timeout <= 0 disables the client-side budget and
// leaves deadlines to the caller's context.
func New(socketPath string, timeout time.Duration) *Client {
	return &Client{socketPath: socketPath, timeout: timeout}
}

// Exec sends one command and returns the trimmed reply. The connection is
// dialed per call because HAProxy closes the runtime socket after each
// command. Replies matching a known error prefix are returned together with
// a non-nil error; other replies are returned as-is for the caller to parse.
func (c *Client) Exec(ctx context.Context, cmd string) (string, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return "", fmt.Errorf("haproxy: dial %s: %w", c.socketPath, err)
	}
	defer conn.Close()

	if dl, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(dl); err != nil {
			return "", fmt.Errorf("haproxy: set deadline: %w", err)
		}
	}
	// Unblock in-flight reads on explicit context cancellation; the deadline
	// above only covers timeout expiry.
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			conn.SetDeadline(time.Now())
		case <-watchDone:
		}
	}()

	if _, err := io.WriteString(conn, cmd+"\n"); err != nil {
		return "", fmt.Errorf("haproxy: write %q: %w", cmd, err)
	}
	raw, err := io.ReadAll(conn)
	if err != nil {
		return "", fmt.Errorf("haproxy: read reply to %q: %w", cmd, err)
	}

	out := strings.TrimSpace(string(raw))
	if isErrorReply(out) {
		return out, fmt.Errorf("haproxy: command %q rejected: %s", cmd, firstLine(out))
	}
	return out, nil
}

// ShowStat samples all frontend rows via "show stat". Column positions are
// resolved from the CSV header by name; svname must be "FRONTEND" and the
// internal "stats" frontend is excluded.
func (c *Client) ShowStat(ctx context.Context) ([]model.FrontendStat, error) {
	out, err := c.Exec(ctx, showStatCmd)
	if err != nil {
		return nil, err
	}
	return parseShowStat(out)
}

// SetMapEntry updates key in the given runtime map. HAProxy replies with
// nothing on success; a missing-key reply triggers a single fallback to
// "add map" so first-time keys are created transparently.
func (c *Client) SetMapEntry(ctx context.Context, mapPath, key, value string) error {
	out, err := c.Exec(ctx, fmt.Sprintf("set map %s %s %s", mapPath, key, value))
	if isMissingEntryReply(out) {
		out, err = c.Exec(ctx, fmt.Sprintf("add map %s %s %s", mapPath, key, value))
		if err != nil {
			return fmt.Errorf("haproxy: add map %s %s: %w", mapPath, key, err)
		}
		if out != "" {
			return fmt.Errorf("haproxy: add map %s %s: unexpected reply: %s", mapPath, key, firstLine(out))
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("haproxy: set map %s %s: %w", mapPath, key, err)
	}
	if out != "" {
		return fmt.Errorf("haproxy: set map %s %s: unexpected reply: %s", mapPath, key, firstLine(out))
	}
	return nil
}

func isErrorReply(out string) bool {
	line := firstLine(out)
	for _, p := range errorReplyPrefixes {
		if strings.HasPrefix(line, p) {
			return true
		}
	}
	return false
}

func isMissingEntryReply(out string) bool {
	l := strings.ToLower(out)
	return strings.Contains(l, "not found") || strings.Contains(l, "unable to find")
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// parseShowStat decodes the "show stat" CSV body. The header line starts
// with "# " and column order is not assumed; "bytes_out" is accepted as an
// alias of HAProxy's native "bout" column name.
func parseShowStat(out string) ([]model.FrontendStat, error) {
	var (
		stats   []model.FrontendStat
		colIdx  map[string]int
		iPxname = -1
		iSvname = -1
		iScur   = -1
		iBytes  = -1
	)

	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			colIdx = make(map[string]int)
			for i, name := range strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "#")), ",") {
				colIdx[strings.TrimSpace(name)] = i
			}
			iPxname = indexOf(colIdx, "pxname")
			iSvname = indexOf(colIdx, "svname")
			iScur = indexOf(colIdx, "scur")
			iBytes = indexOf(colIdx, "bytes_out")
			if iBytes < 0 {
				iBytes = indexOf(colIdx, "bout")
			}
			if iPxname < 0 || iSvname < 0 || iScur < 0 || iBytes < 0 {
				return nil, fmt.Errorf("haproxy: show stat header missing required columns (pxname/svname/scur/bytes_out|bout): %s", line)
			}
			continue
		}
		if colIdx == nil {
			return nil, fmt.Errorf("haproxy: show stat output has no CSV header: %s", firstLine(out))
		}

		fields := strings.Split(line, ",")
		pxname := fieldAt(fields, iPxname)
		svname := fieldAt(fields, iSvname)
		if svname != "FRONTEND" || pxname == "" {
			continue
		}
		if pxname == "stats" { // internal stats frontend is not billable traffic
			continue
		}

		bytesOut, err := parseUintField(fieldAt(fields, iBytes))
		if err != nil {
			return nil, fmt.Errorf("haproxy: frontend %s: bad bytes_out: %w", pxname, err)
		}
		scur, err := parseIntField(fieldAt(fields, iScur))
		if err != nil {
			return nil, fmt.Errorf("haproxy: frontend %s: bad scur: %w", pxname, err)
		}
		stats = append(stats, model.FrontendStat{
			Name:     pxname,
			BytesOut: bytesOut,
			ConnCur:  scur,
		})
	}

	if colIdx == nil {
		return nil, fmt.Errorf("haproxy: empty show stat output")
	}
	return stats, nil
}

func indexOf(m map[string]int, name string) int {
	if i, ok := m[name]; ok {
		return i
	}
	return -1
}

// fieldAt tolerates short rows: an absent trailing field reads as empty.
func fieldAt(fields []string, i int) string {
	if i < 0 || i >= len(fields) {
		return ""
	}
	return strings.TrimSpace(fields[i])
}

func parseUintField(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

func parseIntField(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}
