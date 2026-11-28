// SPDX-FileCopyrightText: 2026 Yifei Sun
// SPDX-License-Identifier: FSL-1.1-ALv2

//go:build linux && !android

package kernel_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"ranet3.com/pkgs/ranet3/control"
	"ranet3.com/pkgs/ranet3/internal/client"
	"ranet3.com/pkgs/ranet3/internal/kernel"
)

func TestSourcedReadbackReachesProductionStatusAdapter(t *testing.T) {
	const readError = "kernel: read IPv6 main routes for sourced destinations: input/output error"
	routes, read := kernel.SourcedReadbackForTest(t)
	adapter := client.ReconcilerOf(routes)
	if got := adapter.Status().Sourced; got != nil {
		t.Fatalf("unobserved policy appeared in public status: %+v", got)
	}
	assertStatus := func(phase string, rules int, state, detail string) {
		t.Helper()
		status := control.Status{Kernel: adapter.Status()}
		sourced := status.Kernel.Sourced
		if sourced == nil || sourced.Rules != rules || sourced.State != state || sourced.Error != detail ||
			len(sourced.Answers) != 1 || (sourced.Answers[0].Kind == "unknown") != (state == "failing") {
			t.Fatalf("%s: adapter lost the fake-backed observation: %+v", phase, sourced)
		}
		body, err := json.Marshal(status)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{`"kernel":{`, fmt.Sprintf(`"rules":%d`, rules), `"state":"` + state + `"`} {
			if !strings.Contains(string(body), want) {
				t.Fatalf("%s: public JSON omitted %q: %s", phase, want, body)
			}
		}
		var long strings.Builder
		control.RenderStatusLong(&long, status)
		if state == "failing" {
			if !strings.Contains(string(body), `"error":"`+detail+`"`) ||
				!strings.Contains(string(body), `"kind":"unknown"`) ||
				!strings.Contains(long.String(), "R unknown") || !strings.Contains(long.String(), detail) ||
				strings.Contains(long.String(), fmt.Sprintf("R %d,", rules)) {
				t.Fatalf("%s: failed readback claimed a current count: %s, %s", phase, body, long.String())
			}
		} else if !strings.Contains(long.String(), fmt.Sprintf("R %d,", rules)) ||
			strings.Contains(long.String(), "R unknown") {
			t.Fatalf("%s: current readback missing from long status: %s", phase, long.String())
		}
	}

	if err := read(false, false); err != nil {
		t.Fatalf("first fake-backed readback: %v", err)
	}
	assertStatus("first readback", 2, "active", "")
	if err := read(true, false); err == nil || err.Error() != readError {
		t.Fatalf("failed fake-backed main read: %v", err)
	}
	assertStatus("cached failure", 2, "failing", readError)
	if err := read(false, true); err != nil {
		t.Fatalf("recovered fake-backed readback: %v", err)
	}
	assertStatus("recovery", 3, "active", "")

	first, firstRead := kernel.SourcedReadbackForTest(t)
	adapter = client.ReconcilerOf(first)
	if err := firstRead(true, false); err == nil || err.Error() != readError {
		t.Fatalf("first failed fake-backed main read: %v", err)
	}
	assertStatus("first failure", 0, "failing", readError)
}
