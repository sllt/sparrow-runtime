package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"testing"
)

func TestServerPortRemainsReservedUntilStartup(t *testing.T) {
	v := setup(t.TempDir(), "file", "unused-server", "")
	t.Cleanup(v.close)
	if v.serverPort == v.sink.port() {
		t.Fatal("server and HTTP sink share the same port")
	}
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", v.serverPort))
	if err == nil {
		_ = l.Close()
		t.Fatal("another fixture could reuse the server port before startup")
	}
}

func TestFileFixtureExistsBeforeStartingTheServer(t *testing.T) {
	v := setup(t.TempDir(), "file", "unused-server", "")
	t.Cleanup(v.close)
	info, err := os.Stat(v.file)
	if err != nil {
		t.Fatalf("input must exist before asynchronous source startup: %v", err)
	}
	if !info.Mode().IsRegular() || info.Size() != 0 {
		t.Fatalf("input must start as an empty regular file: %v", info)
	}
	v.publish(1, 2, "et")
	got, err := os.ReadFile(v.file)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append(row(1, "et").json(), '\n'), append(row(2, "et").json(), '\n')...)
	if !bytes.Equal(got, want) {
		t.Fatalf("publish must append to the provisioned input: %q", got)
	}
}

// The sliding-count oracle's state cuts sit where the driver claims.
func TestSlidingCountOracleCuts(t *testing.T) {
	if n := len(oracle("slide", 4)); n != 0 {
		t.Fatalf("not_full: %d outputs before any key reaches size", n)
	}
	if len(oracle("slide", 8)) != 2 || len(oracle("slide", 7)) != 1 {
		t.Fatal("at_boundary must sit exactly on a trigger")
	}
	if len(oracle("slide", 14)) != len(oracle("slide", 12)) {
		t.Fatal("input_after (slide) must not complete a window")
	}
	for _, e := range oracle("slide", 36) {
		if e.C != 3 || e.Win == "" {
			t.Fatalf("every sliding output covers the last 3 rows with a window: %+v", e)
		}
	}
}

// v33 fixture: the cut selectors find real cuts and the simulation exercises
// late rows, merges, caps and three keys.
func TestETBufferedOracleCuts(t *testing.T) {
	for _, shape := range []string{"sess", "etslide"} {
		out, info := etSim(shape, 36)
		late, merged := 0, 0
		keys := map[string]bool{}
		for _, in := range info {
			if in.late {
				late++
			}
			if in.merged {
				merged++
			}
		}
		for _, e := range out {
			keys[e.Device] = true
			if e.Win == "" {
				t.Fatalf("%s: output without window bounds", shape)
			}
		}
		if late < 3 || len(keys) != 3 || len(out) < 6 {
			t.Fatalf("%s: weak fixture late=%d keys=%d outputs=%d", shape, late, len(keys), len(out))
		}
		if shape == "sess" && merged < 2 {
			t.Fatalf("session fixture never merges")
		}
		k := etStateCut(shape, "about_to_close")
		if info[k-1].open < 2 || len(oracle(shape, k+1)) <= len(oracle(shape, k)) {
			t.Fatalf("%s: about_to_close cut %d does not close on the next row", shape, k)
		}
		p2 := etInputAfter(shape, 13)
		if p2 <= 13 || len(oracle(shape, p2)) != len(oracle(shape, 13)) || len(oracle(shape, p2+1)) == len(oracle(shape, 13)) {
			t.Fatalf("%s: input_after %d not the last row before an output", shape, p2)
		}
		// Prefix property: outputs after k rows are a prefix of outputs after n.
		for n := 0; n <= 36; n++ {
			pre := oracle(shape, n)
			for i := range pre {
				if pre[i].text() != out[i].text() {
					t.Fatalf("%s: prefix %d differs at %d", shape, n, i)
				}
			}
		}
	}
	k := etStateCut("sess", "ooo_merged")
	_, info := etSim("sess", 36)
	if !info[k-1].merged || !info[k].late {
		t.Fatalf("ooo_merged cut %d invalid", k)
	}
	// Hand-checked session: d? rows 1..4 -> first output is d1's session
	// closed by row 4 (ts 4000 >= 0+3500 for key d1 at ts 0).
	got, _ := etSim("sess", 4)
	if len(got) == 0 || got[0].Device != "d1" || got[0].Win != "0|3500" {
		t.Fatalf("hand-checked session close: %+v", got)
	}
}
