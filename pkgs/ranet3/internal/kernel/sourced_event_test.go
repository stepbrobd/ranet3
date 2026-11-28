// SPDX-FileCopyrightText: 2026 Yifei Sun
// SPDX-License-Identifier: FSL-1.1-ALv2

package kernel

import (
	"log/slog"
	"net/netip"
	"testing"

	"ranet3.com/pkgs/ranet3/internal/events"
)

func TestSourcedEventsFollowCommittedChanges(t *testing.T) {
	bus := events.New()
	r, _, _ := harness(t, Table{}, Runtime{Events: bus})
	eventsOf := func() []map[string]string {
		observed := bus.Recorded(func(kind, _ string, _ []slog.Attr) bool { return kind == "kernel.sourced" }, 0)
		out := make([]map[string]string, 0, len(observed))
		for _, event := range observed {
			out = append(out, event.Attrs)
		}
		return out
	}
	r.withdrawSourcedStatus()
	if got := eventsOf(); len(got) != 0 {
		t.Fatalf("never-keyed withdrawal emitted %v", got)
	}
	answer := SourcedAnswer{To: netip.MustParsePrefix("2001:db8:1::/96"), Kind: "refused", Protection: true}
	status := SourcedStatus{Named: 1, State: "active", Rules: 5, Answers: []SourcedAnswer{answer}}
	r.publishSourced(status)
	r.publishSourced(status)
	main := SourcedAnswer{To: answer.To, Kind: "main", MainPrefix: answer.To, MainProtocol: 9}
	status.Answers = []SourcedAnswer{main}
	r.publishSourced(status)
	main.MainProtocol = 10
	status.Answers = []SourcedAnswer{main}
	r.publishSourced(status)
	main.MainPrefix = netip.MustParsePrefix("2001:db8:1::/112")
	status.Answers = []SourcedAnswer{main}
	r.publishSourced(status)
	status.Limited, status.State = true, "limited"
	r.publishSourced(status)
	r.withdrawSourcedStatus()
	r.withdrawSourcedStatus()
	got := eventsOf()
	if len(got) != 6 || got[0]["state"] != "active" || got[0]["rules"] != "5" ||
		got[3]["state"] != "active" || got[4]["state"] != "limited" || got[4]["limited"] != "true" ||
		got[5]["state"] != "off" || got[5]["named"] != "0" {
		t.Fatalf("committed, changed and withdrawn events: %v", got)
	}
}
