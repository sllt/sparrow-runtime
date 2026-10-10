package main

import (
	"bytes"
	"os"
	"testing"
)

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
