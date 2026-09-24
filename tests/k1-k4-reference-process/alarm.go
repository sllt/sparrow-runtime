package main

import (
	"fmt"
	"path/filepath"
)

func runAlarmMatrix(root, serverBin, oldServerBin, natsBin string, fileOnly bool) {
	transports := []string{"file"}
	if !fileOnly {
		transports = append(transports, "jetstream")
	}
	count := 0
	guards := map[string]any{}
	for _, transport := range transports {
		for _, kind := range []string{"alarm", "alarm_immediate"} {
			runTimedProcess(filepath.Join(root, transport+"-"+kind), serverBin, natsBin, kind, transport == "jetstream")
			count++
		}
		source := filepath.Join(root, transport+"-alarm")
		guards[transport] = runProfileGuard(filepath.Join(root, "old-"+transport+"-guard"), oldServerBin,
			filepath.Join(source, "checkpoint"), filepath.Join(source, "signals.ndjson"), "checkpoint source profile mismatch", "old alarm profile guard")
	}
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "process_scenarios": count, "file_only": fileOnly,
		"old_profile_guards": guards, "snapshot_versions": []int{20, 21}, "exactly_once_claimed": false, "certified": false})
	fmt.Println("ALARM_PROCESS_OK")
}
