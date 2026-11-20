// SPDX-FileCopyrightText: 2026 Yifei Sun
// SPDX-License-Identifier: FSL-1.1-ALv2

//go:build linux && !android

package kernel

import (
	"slices"
	"strings"
	"testing"

	"ranet3.com/pkgs/ranet3/internal/netnstest"
	"ranet3.com/pkgs/ranet3/schema"
)

// A start with no reconciler finds the down set an earlier instance left under its protocol in the kernel's own dump, in a namespace
// of its own, and warns once naming each of its rules with the command that deletes it, and nothing of another shape, and leaves
// every rule there
func TestStartWithNoReconcilerWarnsOfTheDownSetLeft(t *testing.T) {
	netnstest.EnterEmpty(t)
	conn, err := dialNetlink()
	if err != nil {
		t.Fatalf("dial rtnetlink: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	const proto = 99
	left := &netlinkPlatform{table: Table{ID: DefaultTable, Proto: proto}, conn: conn}
	down := downRules(prefixes(prefix("192.0.2.0/25"), prefix("3fff::/21")))
	// a rule at the refusal's priority that looks a table up, which no stop writes
	other := Rule{Family: FamilyIPv4, Priority: priorityDownRefused, To: schema.PrefixFrom(prefix("198.51.100.0/24")), Table: 300}
	for _, rule := range append(slices.Clone(down), other) {
		if err := left.AddRule(rule); err != nil {
			t.Fatalf("add %s: %v", rule, err)
		}
	}
	before, err := DumpRules()
	if err != nil {
		t.Fatal(err)
	}
	said := logged(t)
	WarnHeldOverlay()
	text := said.String()
	if n := strings.Count(text, "kernel found rules an earlier instance left to hold overlay prefixes"); n != 1 {
		t.Errorf("the start warned %d times, want once:\n%s", n, text)
	}
	for _, want := range []string{
		"priority 32765 to 192.0.2.0/25 family ipv4 unreachable protocol 99",
		"priority 32764 to 192.0.2.0/25 family ipv4 lookup main suppress_prefixlength 24 protocol 99",
		"priority 32765 to 3fff::/21 family ipv6 unreachable protocol 99",
		"priority 32764 to 3fff::/21 family ipv6 lookup main suppress_prefixlength 20 protocol 99",
		"ip -4 rule del priority 32765 to 192.0.2.0/25 protocol 99",
		"ip -4 rule del priority 32764 to 192.0.2.0/25 protocol 99",
		"ip -6 rule del priority 32765 to 3fff::/21 protocol 99",
		"ip -6 rule del priority 32764 to 3fff::/21 protocol 99",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the warning does not name %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "198.51.100.0/24") {
		t.Errorf("the warning names a rule of another shape:\n%s", text)
	}
	after, err := DumpRules()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after, before) {
		t.Errorf("the start changed the rules from %+v to %+v", before, after)
	}
}
