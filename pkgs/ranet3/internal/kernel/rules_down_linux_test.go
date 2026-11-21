// SPDX-FileCopyrightText: 2026 Yifei Sun
// SPDX-License-Identifier: FSL-1.1-ALv2

//go:build linux && !android

package kernel

import (
	"net/netip"
	"slices"
	"strings"
	"testing"

	"ranet3.com/pkgs/ranet3/internal/netnstest"
	"ranet3.com/pkgs/ranet3/internal/netstack"
	"ranet3.com/pkgs/ranet3/schema"
)

// A start with no reconciler finds the down set an earlier instance left under the default protocol in the kernel's own dump, in a
// namespace of its own, and warns once naming each of its rules with the command that deletes it, and not the same set under
// another protocol or a rule of another shape, and leaves every rule there
func TestStartWithNoReconcilerWarnsOfTheDownSetLeft(t *testing.T) {
	netnstest.EnterEmpty(t)
	conn, err := dialNetlink()
	if err != nil {
		t.Fatalf("dial rtnetlink: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ours := &netlinkPlatform{table: Table{ID: DefaultTable, Proto: DefaultProtocol}, conn: conn}
	another := &netlinkPlatform{table: Table{ID: DefaultTable, Proto: 99}, conn: conn}
	for _, rule := range downRules(downOverlay) {
		if err := ours.AddRule(rule); err != nil {
			t.Fatalf("add %s: %v", rule, err)
		}
		if err := another.AddRule(rule); err != nil {
			t.Fatalf("add %s under another protocol: %v", rule, err)
		}
	}
	// a rule at the refusal's priority that looks a table up, which no stop writes
	other := Rule{Family: FamilyIPv4, Priority: priorityDownRefused, To: schema.PrefixFrom(prefix("198.51.100.0/24")), Table: 300}
	if err := ours.AddRule(other); err != nil {
		t.Fatalf("add %s: %v", other, err)
	}
	before, err := DumpRules()
	if err != nil {
		t.Fatal(err)
	}
	said := logged(t)
	WarnHeldOverlay(downOverlay)
	text := said.String()
	if n := strings.Count(text, downWarning); n != 1 {
		t.Errorf("the start warned %d times, want once:\n%s", n, text)
	}
	for _, want := range downWarned {
		if !strings.Contains(text, want) {
			t.Errorf("the warning does not name %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "198.51.100.0/24") || strings.Contains(text, "protocol 99") {
		t.Errorf("the warning names a rule outside the down set:\n%s", text)
	}
	after, err := DumpRules()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after, before) {
		t.Errorf("the start changed the rules from %+v to %+v", before, after)
	}
}

// A stop through a real kernel leaves the down set under the reconciler's own protocol, which the fake's rules do not carry, as the
// only rules of that protocol, so a start of the same protocol lists and deletes them
func TestNetlinkStopLeavesTheDownSetUnderItsProtocol(t *testing.T) {
	const device = "ranetdown0"
	plat := rootRulePlatform(t, device)
	tbl, rt := platformFor(device)
	rt.Overlay = downOverlay
	routes := netstack.NewRouteTable()
	routes.Set(netip.Prefix{}, prefix("192.0.2.7/32"), nil)
	routes.Set(netip.Prefix{}, prefix("3fff::7/128"), nil)
	r := newReconciler(tbl.Normalized(), rt, nil, routes, plat)
	// the tun is down, so the pass's routes fail, and the stop takes the rules the pass added
	_ = r.reconcile()
	if got := r.DerivedRules(); len(got) == 0 {
		t.Fatal("the pass derived no rules, so the stop proves nothing")
	}
	if err := r.withdraw(errStopped); err != nil {
		t.Fatal(err)
	}
	dumped, err := DumpRules()
	if err != nil {
		t.Fatal(err)
	}
	var ours []DumpedRule
	for _, rule := range dumped {
		if rule.Protocol == DefaultProtocol {
			ours = append(ours, rule)
		}
	}
	want := dumpedDown(downOverlay, DefaultProtocol)
	if !sameRules(ours, want) {
		t.Errorf("the stop left the rules %+v under protocol %d, want the down set %+v", ours, DefaultProtocol, want)
	}
}
