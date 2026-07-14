package haproxy

import (
	"bufio"
	"context"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer emulates the HAProxy runtime socket: one command per
// connection, canned reply (indexed by accept order), then close. With
// hang=true it accepts and reads but never replies or closes.
type fakeServer struct {
	path    string
	ln      net.Listener
	replies []string
	hang    bool

	mu    sync.Mutex
	cmds  []string
	conns []net.Conn

	done      chan struct{}
	closeOnce sync.Once
}

func newFakeServer(t *testing.T, replies ...string) *fakeServer {
	return startFakeServer(t, false, replies...)
}

// newSilentServer accepts and reads commands but never replies or closes.
func newSilentServer(t *testing.T) *fakeServer {
	return startFakeServer(t, true)
}

func startFakeServer(t *testing.T, hang bool, replies ...string) *fakeServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "h.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen %s: %v", path, err)
	}
	s := &fakeServer{path: path, ln: ln, replies: replies, hang: hang, done: make(chan struct{})}
	go s.loop()
	t.Cleanup(s.close)
	return s
}

func (s *fakeServer) loop() {
	for i := 0; ; i++ {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()

		line, _ := bufio.NewReader(conn).ReadString('\n')
		if line != "" {
			s.mu.Lock()
			s.cmds = append(s.cmds, strings.TrimSuffix(line, "\n"))
			s.mu.Unlock()
		}
		if s.hang {
			<-s.done
			return
		}
		if i < len(s.replies) {
			io.WriteString(conn, s.replies[i])
		}
		conn.Close()
	}
}

func (s *fakeServer) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.ln.Close()
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, c := range s.conns {
			c.Close()
		}
	})
}

func (s *fakeServer) commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.cmds...)
}

const statCSV = `# pxname,svname,qcur,qmax,scur,smax,slim,stot,bin,bytes_out,dreq,status,
env-a,FRONTEND,,,42,100,2000,12345,999,1048576,0,OPEN,
env-a,web1,0,0,3,10,,12345,999,262144,0,UP,
env-a,BACKEND,0,0,3,10,200,12345,999,262144,0,UP,
stats,FRONTEND,,,1,1,10,5,10,2048,0,OPEN,
env-b,FRONTEND,,,7,50,1000,555,111,73400320,0,OPEN,
`

func TestShowStat_FrontendFilteringAndParsing(t *testing.T) {
	srv := newFakeServer(t, statCSV)
	c := New(srv.path, time.Second)

	stats, err := c.ShowStat(context.Background())
	if err != nil {
		t.Fatalf("ShowStat: %v", err)
	}

	if got := srv.commands(); len(got) != 1 || got[0] != "show stat -1 1 -1" {
		t.Fatalf("sent commands = %q, want [show stat -1 1 -1]", got)
	}
	if len(stats) != 2 {
		t.Fatalf("got %d frontends, want 2 (BACKEND/server/stats rows must be dropped): %+v", len(stats), stats)
	}
	if stats[0].Name != "env-a" || stats[0].BytesOut != 1048576 || stats[0].ConnCur != 42 {
		t.Errorf("stats[0] = %+v, want {env-a 1048576 42}", stats[0])
	}
	if stats[1].Name != "env-b" || stats[1].BytesOut != 73400320 || stats[1].ConnCur != 7 {
		t.Errorf("stats[1] = %+v, want {env-b 73400320 7}", stats[1])
	}
}

func TestShowStat_HeaderOrderIndependence(t *testing.T) {
	// Same data as statCSV's frontends but with shuffled column order: the
	// parser must resolve indexes from the header, not from fixed positions.
	shuffled := `# svname,bytes_out,slim,pxname,scur,status,
FRONTEND,1048576,2000,env-a,42,OPEN,
web1,262144,,env-a,3,UP,
FRONTEND,73400320,1000,env-b,7,OPEN,
`
	srv := newFakeServer(t, shuffled)
	c := New(srv.path, time.Second)

	stats, err := c.ShowStat(context.Background())
	if err != nil {
		t.Fatalf("ShowStat: %v", err)
	}
	if len(stats) != 2 {
		t.Fatalf("got %d frontends, want 2: %+v", len(stats), stats)
	}
	if stats[0].Name != "env-a" || stats[0].BytesOut != 1048576 || stats[0].ConnCur != 42 {
		t.Errorf("stats[0] = %+v, want {env-a 1048576 42}", stats[0])
	}
	if stats[1].Name != "env-b" || stats[1].BytesOut != 73400320 || stats[1].ConnCur != 7 {
		t.Errorf("stats[1] = %+v, want {env-b 73400320 7}", stats[1])
	}
}

func TestShowStat_BoutAliasAccepted(t *testing.T) {
	// Real HAProxy names the column "bout"; the parser must accept it.
	csv := `# pxname,svname,scur,bout,
env-a,FRONTEND,5,4096,
`
	srv := newFakeServer(t, csv)
	c := New(srv.path, time.Second)

	stats, err := c.ShowStat(context.Background())
	if err != nil {
		t.Fatalf("ShowStat: %v", err)
	}
	if len(stats) != 1 || stats[0].BytesOut != 4096 || stats[0].ConnCur != 5 {
		t.Fatalf("stats = %+v, want [{env-a 4096 5}]", stats)
	}
}

func TestShowStat_EmptyFieldsTreatedAsZero(t *testing.T) {
	csv := `# pxname,svname,scur,bytes_out,
env-a,FRONTEND,,,
env-b,FRONTEND,3,,
`
	srv := newFakeServer(t, csv)
	c := New(srv.path, time.Second)

	stats, err := c.ShowStat(context.Background())
	if err != nil {
		t.Fatalf("ShowStat: %v", err)
	}
	if len(stats) != 2 {
		t.Fatalf("got %d frontends, want 2: %+v", len(stats), stats)
	}
	if stats[0].BytesOut != 0 || stats[0].ConnCur != 0 {
		t.Errorf("stats[0] = %+v, want zero BytesOut/ConnCur", stats[0])
	}
	if stats[1].BytesOut != 0 || stats[1].ConnCur != 3 {
		t.Errorf("stats[1] = %+v, want BytesOut=0 ConnCur=3", stats[1])
	}
}

func TestShowStat_ErrorReply(t *testing.T) {
	srv := newFakeServer(t, "Unknown command. Please enter one of the following commands only:\nhelp\n")
	c := New(srv.path, time.Second)

	if _, err := c.ShowStat(context.Background()); err == nil {
		t.Fatal("ShowStat succeeded on an 'Unknown command' reply, want error")
	}
}

func TestExec_TrimsReplyAndReportsErrorPrefixes(t *testing.T) {
	srv := newFakeServer(t, "  some output\n\n", "Permission denied\n")
	c := New(srv.path, time.Second)

	out, err := c.Exec(context.Background(), "show info")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if out != "some output" {
		t.Errorf("Exec output = %q, want %q", out, "some output")
	}

	out, err = c.Exec(context.Background(), "show info")
	if err == nil {
		t.Fatal("Exec succeeded on 'Permission denied' reply, want error")
	}
	if out != "Permission denied" {
		t.Errorf("Exec raw output = %q, want it returned alongside the error", out)
	}
}

func TestSetMapEntry_EmptyReplyIsSuccess(t *testing.T) {
	srv := newFakeServer(t, "")
	c := New(srv.path, time.Second)

	if err := c.SetMapEntry(context.Background(), "/etc/haproxy/bwlim.map", "env-a", "1250000"); err != nil {
		t.Fatalf("SetMapEntry: %v", err)
	}
	got := srv.commands()
	want := []string{"set map /etc/haproxy/bwlim.map env-a 1250000"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("sent commands = %q, want %q", got, want)
	}
}

func TestSetMapEntry_FallsBackToAddMap(t *testing.T) {
	srv := newFakeServer(t, "entry not found.\n", "")
	c := New(srv.path, time.Second)

	if err := c.SetMapEntry(context.Background(), "/m.map", "env-a", "125"); err != nil {
		t.Fatalf("SetMapEntry with add-map fallback: %v", err)
	}
	got := srv.commands()
	want := []string{"set map /m.map env-a 125", "add map /m.map env-a 125"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("sent commands = %q, want %q", got, want)
	}
}

func TestSetMapEntry_UnableToFindTriggersFallback(t *testing.T) {
	srv := newFakeServer(t, "unable to find entry\n", "")
	c := New(srv.path, time.Second)

	if err := c.SetMapEntry(context.Background(), "/m.map", "k", "v"); err != nil {
		t.Fatalf("SetMapEntry: %v", err)
	}
	if got := srv.commands(); len(got) != 2 || !strings.HasPrefix(got[1], "add map ") {
		t.Fatalf("sent commands = %q, want set map then add map", got)
	}
}

func TestSetMapEntry_AddMapFailurePropagates(t *testing.T) {
	srv := newFakeServer(t, "entry not found.\n", "Unknown map identifier.\n")
	c := New(srv.path, time.Second)

	err := c.SetMapEntry(context.Background(), "/m.map", "k", "v")
	if err == nil {
		t.Fatal("SetMapEntry succeeded although add map was rejected, want error")
	}
	if !strings.Contains(err.Error(), "add map") {
		t.Errorf("error %q does not mention the failing add map step", err)
	}
}

func TestSetMapEntry_UnexpectedReplyIsError(t *testing.T) {
	srv := newFakeServer(t, "malformed value\n")
	c := New(srv.path, time.Second)

	if err := c.SetMapEntry(context.Background(), "/m.map", "k", "v"); err == nil {
		t.Fatal("SetMapEntry succeeded on a non-empty non-missing reply, want error")
	}
	if got := srv.commands(); len(got) != 1 {
		t.Fatalf("sent %d commands, want 1 (no add-map fallback): %q", len(got), got)
	}
}

func TestExec_TimeoutOnSilentServer(t *testing.T) {
	srv := newSilentServer(t)
	c := New(srv.path, 150*time.Millisecond)

	start := time.Now()
	_, err := c.Exec(context.Background(), "show stat")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Exec returned no error although the server never replied")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Exec took %v, want it bounded by the 150ms budget", elapsed)
	}
}

func TestExec_ContextCancellationUnblocksRead(t *testing.T) {
	srv := newSilentServer(t)
	c := New(srv.path, 0) // no client budget: cancellation must do the work

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := c.Exec(ctx, "show stat")
	if err == nil {
		t.Fatal("Exec returned no error after context cancellation")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Exec took %v after cancel, want prompt return", elapsed)
	}
}

func TestExec_DialFailure(t *testing.T) {
	c := New(filepath.Join(t.TempDir(), "absent.sock"), 200*time.Millisecond)
	if _, err := c.Exec(context.Background(), "show stat"); err == nil {
		t.Fatal("Exec succeeded against a non-existent socket, want dial error")
	}
}
