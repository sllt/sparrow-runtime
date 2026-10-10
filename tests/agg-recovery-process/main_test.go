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
