package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The timeline rebuild keeps only committed history across a crash and
// appends the restored process (including the replayed pending decision).
func TestTimelineTruncatesToRestoredCutAndReplays(t *testing.T) {
	dir := t.TempDir()
	log := strings.Join([]string{
		"10 start 0 0", "10 tick 100 0", "10 rows 100 1", "10 tick 200 0", "10 rows 200 1", "10 tick 300 0", "10 rows 300 1",
		"11 start 200 0", "11 tick 300 0", "11 rows 300 1", "11 tick 350 0", "11 tick 420 0",
	}, "\n") + "\n"
	p := filepath.Join(dir, "pt_clock.log")
	if err := os.WriteFile(p, []byte(log), 0600); err != nil {
		t.Fatal(err)
	}
	tl := parseTimeline(p)
	if tl.rows() != 3 || tl.lastTick() != 420 || len(tl.cuts) != 2 || tl.cuts[1] != 200 || tl.rowsAt(200) != 2 {
		t.Fatalf("timeline %+v", tl)
	}
	if tl.firstNew[0] != 350 {
		t.Fatalf("first new tick after restore: %v", tl.firstNew)
	}
}

func TestRestoredCutMustBeALoggedDecision(t *testing.T) {
	p := filepath.Join(t.TempDir(), "pt_clock.log")
	_ = os.WriteFile(p, []byte("1 start 0 0\n1 tick 100 0\n2 start 150 0\n"), 0600)
	defer func() {
		if recover() == nil {
			t.Fatal("cut at an unlogged time accepted")
		}
	}()
	parseTimeline(p)
}

func fixture(times []int64) timeline {
	var tl timeline
	tl.cuts = []int64{0}
	for _, m := range times {
		tl.entries = append(tl.entries, entry{true, m, 0}, entry{false, m, 1})
	}
	for k := 1; k <= 40; k++ {
		tl.entries = append(tl.entries, entry{true, times[len(times)-1] + int64(k)*100000, 0})
	}
	return tl
}

// Hand-checked windows: negative hop starts, session gap/maxd cap, sliding
// delay exclusion at the deadline tick, tumbling boundaries.
func TestOracleShapes(t *testing.T) {
	// rows: 1:a@120000 2:a@180000 3:b@330000 4:a@380000 5:a@640000
	tl := fixture([]int64{120000, 180000, 330000, 380000, 640000})
	got := func(shape string) string {
		var s []string
		for _, w := range oracle(shape, tl) {
			if w.emit == math.MaxInt64 {
				t.Fatalf("%s window never closes", shape)
			}
			s = append(s, w.text(shape))
		}
		return strings.Join(s, " ")
	}
	hop := got("pthop")
	for _, want := range []string{"a|-100000|200000|2|8", "a|0|300000|2|8", "a|100000|400000|3|17", "b|100000|400000|1|7"} {
		if !strings.Contains(hop, want) {
			t.Fatalf("hop missing %s in %s", want, hop)
		}
	}
	sess := got("ptsess")
	// a: 120000 -> 180000 (end 430000) -> 380000 merges (end min(630000, 620000)=620000), 640000 new session.
	for _, want := range []string{"a|120000|620000|3|17|3|9", "a|640000|890000|1|11|11|11", "b|330000|580000|1|7|7|7"} {
		if !strings.Contains(sess, want) {
			t.Fatalf("session missing %s in %s", want, sess)
		}
	}
	slide := got("ptslide")
	if !strings.Contains(slide, "a|-179999|240001|2|8|3|5") || !strings.Contains(slide, "a|80001|500001|3|17|3|9") {
		t.Fatalf("sliding %s", slide)
	}
	tum := got("pttumble")
	if !strings.Contains(tum, "a|0|200000|2|8|3|5") || !strings.Contains(tum, "a|200000|400000|1|9|9|9") {
		t.Fatalf("tumble %s", tum)
	}
	fmt.Fprintln(os.Stderr, hop)
}
