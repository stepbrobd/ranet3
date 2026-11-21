// SPDX-FileCopyrightText: 2026 Yifei Sun
// SPDX-License-Identifier: FSL-1.1-ALv2

package kernel

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"ranet3.com/pkgs/ranet3/internal/netstack"
	"ranet3.com/pkgs/ranet3/schema"
)

// downOverlay is the overlay of the down set's tests, a prefix of each family
var downOverlay = prefixes(prefix("192.0.2.0/25"), prefix("3fff::/21"))

// kernelWrite is one write the fake took: a route deleted, or a rule added or deleted
type kernelWrite struct {
	op    string
	rule  Rule
	route Route
}

// recordingKernel is the fake kernel noting each write it took, in the order taken
type recordingKernel struct {
	*fakeKernel
	noted  sync.Mutex
	writes []kernelWrite
}

func (k *recordingKernel) note(w kernelWrite) {
	k.noted.Lock()
	defer k.noted.Unlock()
	k.writes = append(k.writes, w)
}

// taken is every write noted so far
func (k *recordingKernel) taken() []kernelWrite {
	k.noted.Lock()
	defer k.noted.Unlock()
	return slices.Clone(k.writes)
}

func (k *recordingKernel) DelRoute(route Route) error {
	err := k.fakeKernel.DelRoute(route)
	if err == nil {
		k.note(kernelWrite{op: "delete route", route: route})
	}
	return err
}

func (k *recordingKernel) AddRule(rule Rule) error {
	err := k.fakeKernel.AddRule(rule)
	if err == nil {
		k.note(kernelWrite{op: "add rule", rule: rule})
	}
	return err
}

func (k *recordingKernel) DelRule(rule Rule) error {
	err := k.fakeKernel.DelRule(rule)
	if err == nil {
		k.note(kernelWrite{op: "delete rule", rule: rule})
	}
	return err
}

// holds reports whether the fake's rules are exactly rules
func (k *recordingKernel) holds(rules []Rule) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	want := map[Rule]bool{}
	for _, rule := range rules {
		want[rule] = true
	}
	return maps.Equal(k.rules, want)
}

// holdsAny reports whether the fake holds any of rules
func (k *recordingKernel) holdsAny(rules []Rule) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.ContainsFunc(rules, func(rule Rule) bool { return k.rules[rule] })
}

// downHarness is a reconciler over downOverlay on the recording fake, with a live leaf in each family
func downHarness(t *testing.T) (*Reconciler, *recordingKernel) {
	t.Helper()
	tbl := Table{Reconcile: schema.Duration(time.Minute)}.Normalized()
	table := netstack.NewRouteTable()
	table.Set(netip.Prefix{}, prefix("192.0.2.7/32"), nil)
	table.Set(netip.Prefix{}, prefix("3fff::7/128"), nil)
	k := &recordingKernel{fakeKernel: newFakeKernel(t)}
	return newReconciler(tbl, Runtime{Interface: "ranet0", Overlay: downOverlay}, nil, table, k), k
}

// runDown runs r until its derived rules stand, and returns the cancel of its run and a wait for its end
func runDown(t *testing.T, r *Reconciler, k *recordingKernel) (context.CancelCauseFunc, func()) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	wait := sync.OnceFunc(func() {
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run: %v", err)
			}
		case <-time.After(waitBudget):
			t.Error("run did not return after its cancel")
		}
	})
	t.Cleanup(func() {
		cancel(nil)
		wait()
	})
	waitFor(t, func() bool { return k.holdsDerived(r) && !k.holdsAny(downRules(downOverlay)) })
	return cancel, wait
}

// stopOrder fails where writes, a stop's, do not add each rule of the down set, each prefix's refusal ahead of its lookup of
// main, before their first delete
func stopOrder(t *testing.T, writes []kernelWrite) {
	t.Helper()
	added := map[Rule]int{}
	first := len(writes)
	for i, w := range writes {
		switch w.op {
		case "add rule":
			added[w.rule] = i
		default:
			first = min(first, i)
		}
	}
	for _, rule := range downRules(downOverlay) {
		i, ok := added[rule]
		switch {
		case !ok:
			t.Errorf("the stop never added %s", rule)
		case i > first:
			t.Errorf("the stop added %s after its first delete, %+v", rule, writes[first])
		case rule.Priority == priorityDownMain:
			if refusal := (Rule{Family: rule.Family, Priority: priorityDownRefused, To: rule.To, action: actionUnreachable}); added[refusal] > i {
				t.Errorf("the stop added %s ahead of %s", rule, refusal)
			}
		}
	}
}

// Each stop but the operator's adds the down set before it deletes any route or rule, which is all the fake notes of a stop's
// writes, and leaves it as the reconciler's only rules, counted in the record of a down's withdrawal. A stop whose add of one rule
// of the down set fails keeps that family as stopWhoseAddFails says.
func TestStopAddsTheDownSetBeforeAnyRouteOrRuleLeaves(t *testing.T) {
	for _, c := range []struct {
		name string
		stop func(*Reconciler, context.CancelCauseFunc)
	}{
		{"ranet3 down", func(r *Reconciler, _ context.CancelCauseFunc) { r.SetEnabled(false) }},
		{"a clean stop", func(_ *Reconciler, cancel context.CancelCauseFunc) { cancel(nil) }},
		{"a stop for a failed mesh", func(_ *Reconciler, cancel context.CancelCauseFunc) { cancel(ErrMeshFailed) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, k := downHarness(t)
			cancel, _ := runDown(t, r, k)
			from := len(k.taken())
			c.stop(r, cancel)
			waitFor(t, func() bool { return k.holds(downRules(downOverlay)) && len(k.snapshot()) == 0 })
			stopOrder(t, k.taken()[from:])
			if c.name == "ranet3 down" {
				waitFor(t, func() bool { return r.Stats().Held == 2 })
			}
		})
		for _, failing := range downRules(downOverlay) {
			t.Run(fmt.Sprintf("%s whose add of %s fails", c.name, failing), func(t *testing.T) { stopWhoseAddFails(t, failing, c.name) })
		}
	}
}

// ranet3 disable reconciler withdraws the down set with the rest, of a running reconciler and of one a down stopped, and adds none,
// and a clean stop of a disabled reconciler adds none either, while a start makes the next stop hold again
func TestDisableTakesTheDownSet(t *testing.T) {
	r, k := downHarness(t)
	cancel, wait := runDown(t, r, k)
	down := downRules(downOverlay)
	disable := func(what string) {
		t.Helper()
		from := len(k.taken())
		r.DropHold()
		r.SetEnabled(false)
		waitFor(t, func() bool { return k.holds(nil) && len(k.snapshot()) == 0 })
		for _, w := range k.taken()[from:] {
			if w.op == "add rule" {
				t.Errorf("disable %s added %s", what, w.rule)
			}
		}
		waitFor(t, func() bool { return r.Stats().Held == 0 })
	}
	start := func() {
		t.Helper()
		r.SetEnabled(true)
		waitFor(t, func() bool { return k.holdsDerived(r) && !k.holdsAny(down) })
	}

	disable("of a running reconciler")
	start()
	r.SetEnabled(false)
	waitFor(t, func() bool { return k.holds(down) })
	disable("of a reconciler a down stopped")
	start()
	cancel(nil)
	wait()
	if !k.holds(down) {
		t.Errorf("a clean stop after a start left %v, want the down set", k.rules)
	}

	r, k = downHarness(t)
	cancel, wait = runDown(t, r, k)
	r.DropHold()
	r.SetEnabled(false)
	waitFor(t, func() bool { return k.holds(nil) })
	cancel(nil)
	wait()
	if !k.holds(nil) {
		t.Errorf("a clean stop of a disabled reconciler left %v, want nothing", k.rules)
	}
}

// A start keeps the down set a stop left, for its overlay and for prefixes an earlier overlay held, a length changed among them,
// until its pass added every rule it derives, 5110 among them, and holds a route at each overlay prefix, and then deletes it
func TestStartTakesTheDownSetOnceItsRulesStand(t *testing.T) {
	for _, c := range []struct {
		name string
		left []schema.Prefix
	}{
		{"this overlay's", downOverlay},
		{"this overlay's and a prefix it no longer holds", append(slices.Clone(downOverlay), schema.MustPrefix("198.51.100.0/24"))},
		{"an overlay whose prefixes were shorter", prefixes(prefix("192.0.2.0/26"), prefix("3fff::/22"))},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, k := downHarness(t)
			left := downRules(c.left)
			for _, rule := range left {
				k.rules[rule] = true
			}
			if err := r.reconcile(); err != nil {
				t.Fatal(err)
			}
			writes := k.taken()
			last := -1
			for i, w := range writes {
				if w.op == "add rule" && w.rule.Priority == priorityOverlay {
					last = i
				}
			}
			if last < 0 {
				t.Fatalf("the start added no rule at %d: %+v", priorityOverlay, writes)
			}
			for i, w := range writes {
				if w.op == "delete rule" && slices.Contains(left, w.rule) && i < last {
					t.Errorf("the start deleted %s ahead of its last add at %d, %+v", w.rule, priorityOverlay, writes[last])
				}
			}
			if !k.holdsDerived(r) || k.holdsAny(left) {
				t.Errorf("after the start the kernel holds %v, want the derived rules alone", k.rules)
			}
			if held := r.Stats().Held; held != 0 {
				t.Errorf("the start's record counts %d overlay prefixes held, want 0", held)
			}
		})
	}
}

// A start whose add of 5110 fails, or whose route at an overlay prefix the kernel refuses, keeps the down set, counted in its
// record, and the pass after the failure deletes it
func TestStartWhoseWritesFailKeepsTheDownSet(t *testing.T) {
	for _, c := range []struct {
		name       string
		fail, heal func(*fakeKernel)
	}{
		{"its add of 5110", func(f *fakeKernel) { f.failAddRule[downOverlayRule] = errors.New("netlink says no") }, func(f *fakeKernel) { delete(f.failAddRule, downOverlayRule) }},
		{"its route at an overlay prefix", func(f *fakeKernel) { f.failAdd[downOverlayHold] = errors.New("netlink says no") }, func(f *fakeKernel) { delete(f.failAdd, downOverlayHold) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, k := downHarness(t)
			down := downRules(downOverlay)
			for _, rule := range down {
				k.rules[rule] = true
			}
			c.fail(k.fakeKernel)
			if err := r.reconcile(); err == nil {
				t.Fatal("the pass reported no error")
			}
			if !k.holdsAny(down) || !k.holds(slices.DeleteFunc(append(slices.Clone(down), derivedRules(r.table.ID, r.rt.Overlay, r.ownPrefixes(r.src.Snapshot()), exitView{})...), func(rule Rule) bool { return k.failAddRule[rule] != nil })) {
				t.Errorf("after the failed pass the kernel holds %v, want the down set and every derived rule that went in", k.rules)
			}
			if held := r.Stats().Held; held != 2 {
				t.Errorf("the failed pass's record counts %d overlay prefixes held, want 2", held)
			}
			c.heal(k.fakeKernel)
			if err := r.reconcile(); err != nil {
				t.Fatal(err)
			}
			if !k.holdsDerived(r) || k.holdsAny(down) {
				t.Errorf("after the pass that healed the kernel holds %v, want the derived rules alone", k.rules)
			}
		})
	}
}

// downOverlayRule is 5110 for downOverlay's IPv4 prefix, and downOverlayHold the overlay's hold there
var (
	downOverlayRule = Rule{Family: FamilyIPv4, Priority: priorityOverlay, To: schema.PrefixFrom(prefix("192.0.2.0/25")), Table: DefaultTable}
	downOverlayHold = Route{Destination: prefix("192.0.2.0/25"), Metric: holdMetric, Unreachable: true}
)

// downWarned names each rule of the down set of downOverlay and the command that deletes it, as the warning of a start with no
// reconciler does
var downWarned = []string{
	"priority 32765 to 192.0.2.0/25 family ipv4 unreachable protocol 155",
	"priority 32764 to 192.0.2.0/25 family ipv4 lookup main suppress_prefixlength 24 protocol 155",
	"priority 32765 to 3fff::/21 family ipv6 unreachable protocol 155",
	"priority 32764 to 3fff::/21 family ipv6 lookup main suppress_prefixlength 20 protocol 155",
	"ip -4 rule del priority 32765 to 192.0.2.0/25 protocol 155",
	"ip -4 rule del priority 32764 to 192.0.2.0/25 protocol 155",
	"ip -6 rule del priority 32765 to 3fff::/21 protocol 155",
	"ip -6 rule del priority 32764 to 3fff::/21 protocol 155",
}

// downWarning is the message of that warning
const downWarning = "kernel found rules an earlier instance left to hold overlay prefixes"

// The warning of a start with no reconciler names each rule of its overlay's down set an earlier instance left under the default
// protocol, with the command that deletes it, once for them all, and nothing else: the down set's shape at a prefix the file does
// not hold, the down set under another protocol, a rule at the refusal's priority that looks a table up, and a main lookup whose
// suppress is not the prefix's length less one. With none of the down set it says nothing.
func TestStartWithNoReconcilerNamesEachRuleOfTheDownSetLeft(t *testing.T) {
	others := []DumpedRule{
		Rule{Family: FamilyIPv4, Priority: priorityDownRefused, To: schema.PrefixFrom(prefix("198.51.100.0/24")), action: actionUnreachable}.dumped(DefaultProtocol),
		Rule{Family: FamilyIPv4, Priority: priorityDownRefused, To: schema.PrefixFrom(prefix("192.0.2.0/25")), Table: 300}.dumped(DefaultProtocol),
		Rule{Family: FamilyIPv4, Priority: priorityDownMain, To: schema.PrefixFrom(prefix("192.0.2.0/25")), Table: schema.TableMain, suppress: SuppressUpTo(0)}.dumped(DefaultProtocol),
	}
	others = append(others, DownSet(downOverlay, 99)...)
	said := logged(t)
	warnOfDownSet(append(DownSet(downOverlay, DefaultProtocol), others...), downOverlay)
	text := said.String()
	if n := strings.Count(text, downWarning); n != 1 {
		t.Errorf("the start warned %d times, want once:\n%s", n, text)
	}
	for _, want := range downWarned {
		if !strings.Contains(text, want) {
			t.Errorf("the warning does not name %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "198.51.100.0/24") || strings.Contains(text, "lookup 300") || strings.Contains(text, "suppress_prefixlength 0") || strings.Contains(text, "protocol 99") {
		t.Errorf("the warning names a rule outside the down set:\n%s", text)
	}
	said.Reset()
	warnOfDownSet(others, downOverlay)
	if said.Len() != 0 {
		t.Errorf("with none of the down set the start warned:\n%s", said.String())
	}
}

// A stop deletes the reconciler's rules before any route, from the last priority to the first with each goto ahead of the nop it
// lands on, so 5110 and the overlay's holds go after every rule that looks main up, and the routes go once no rule looks the
// mesh's table up
func TestStopDeletesTheRulesFromTheLastAndTheRoutesAfterThem(t *testing.T) {
	r, k := downHarness(t)
	runDown(t, r, k)
	routes := len(k.snapshot())
	from := len(k.taken())
	r.SetEnabled(false)
	waitFor(t, func() bool { return k.holds(downRules(downOverlay)) && len(k.snapshot()) == 0 })
	var got []string
	for _, w := range k.taken()[from:] {
		switch w.op {
		case "delete route":
			got = append(got, "delete route")
		default:
			got = append(got, fmt.Sprintf("%s %d", w.op, w.rule.Priority))
		}
	}
	want := []string{"add rule 32765", "add rule 32764", "add rule 32765", "add rule 32764"}
	for _, priority := range []int{priorityMesh, priorityMain, priorityUnderlayPastMesh, priorityMeshEnd, priorityOverlay, priorityUnderlayPastOverlay, priorityOverlayEnd} {
		want = append(want, fmt.Sprintf("delete rule %d", priority), fmt.Sprintf("delete rule %d", priority))
	}
	for range routes {
		want = append(want, "delete route")
	}
	if !slices.Equal(got, want) {
		t.Errorf("the stop wrote\n%q\nwant\n%q", got, want)
	}
}

// In every mode the order a stop deletes in puts each rule that looks main up ahead of 5110, each goto just ahead of the nop it
// lands on, and every other rule after the rules of later priorities
func TestWithdrawOrderTakesTheRulesThatLookMainUpAheadOf5110(t *testing.T) {
	links := NetworkState{
		IPv4: NetworkFamily{Links: []Link{{Name: "eth0", Prefixes: []netip.Prefix{prefix("198.51.100.10/24")}}}},
		IPv6: NetworkFamily{Links: []Link{{Name: "eth0", Prefixes: []netip.Prefix{prefix("2001:db8:a::10/64")}}}},
	}
	own := []netip.Prefix{prefix("203.0.113.240/28"), prefix("2001:db8:f0::/48")}
	for _, c := range []struct {
		name string
		exit exitView
	}{
		{"mode off", exitView{}},
		{"mode 2", exitView{on: true, network: links, forwarded: []string{"eth1"}}},
		{"mode 2 under fallback none", exitView{on: true, network: links, forwarded: []string{"eth1"}, fallback: true, own: own}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rules := derivedRules(DefaultTable, downOverlay, own, c.exit)
			order := withdrawOrder(rules)
			if len(order) != len(rules) {
				t.Fatalf("the order holds %d rules of %d", len(order), len(rules))
			}
			last := slices.IndexFunc(order, func(rule Rule) bool { return rule.Priority == priorityOverlay })
			for i, rule := range order {
				switch {
				case rule.action == actionLookup && rule.Table == schema.TableMain && i > last:
					t.Errorf("%s goes after 5110, at %d of %v", rule, i, order)
				case rule.action == actionGoto:
					// the next rule of its family, which a lookup of that family meets next
					next := slices.IndexFunc(order[i+1:], func(other Rule) bool { return other.Family == rule.Family })
					if next < 0 || order[i+1+next].action != actionNop || order[i+1+next].Priority != rule.target {
						t.Errorf("%s is not just ahead of the nop it lands on in %v", rule, order)
					}
				case rule.action != actionNop && i > 0 && order[i-1].action != actionNop && order[i-1].Priority < rule.Priority:
					t.Errorf("%s goes after %s in %v", rule, order[i-1], order)
				}
			}
		})
	}
}

// stopWhoseAddFails is a stop of the kind stop names whose add of failing fails. It keeps the routes and rules of failing's family
// as a kill leaves them and withdraws the other's, and says so: a down records the error with the other family's prefix held and
// adds the rule again on its retry, and a clean stop and a stop for a failed mesh return it, leaving that family as the last pass
// left it.
func stopWhoseAddFails(t *testing.T, failing Rule, stop string) {
	t.Helper()
	r, k := downHarness(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() { cancel(nil); <-done })
	waitFor(t, func() bool { return k.holdsDerived(r) && !k.holdsAny(downRules(downOverlay)) })
	family := func(rules map[Rule]bool, routes []Route) (map[Rule]bool, []Route) {
		kept := map[Rule]bool{}
		for rule := range rules {
			if rule.Family == failing.Family && !isDownRule(rule) {
				kept[rule] = true
			}
		}
		return kept, slices.DeleteFunc(slices.Clone(routes), func(route Route) bool { return familyOf(route.Destination.Addr()) != failing.Family })
	}
	k.mu.Lock()
	rules := maps.Clone(k.rules)
	k.mu.Unlock()
	wantRules, wantRoutes := family(rules, k.snapshot())
	other := slices.DeleteFunc(downRules(downOverlay), func(rule Rule) bool { return rule.Family == failing.Family })
	k.mu.Lock()
	k.failAddRule[failing] = errors.New("netlink says no")
	k.mu.Unlock()
	leftAsKilled := func() bool {
		k.mu.Lock()
		now := maps.Clone(k.rules)
		k.mu.Unlock()
		gotRules, gotRoutes := family(now, k.snapshot())
		for _, rule := range other {
			if !now[rule] {
				return false
			}
		}
		others := 0
		for rule := range now {
			if rule.Family != failing.Family && !slices.Contains(other, rule) {
				others++
			}
		}
		return others == 0 && maps.Equal(gotRules, wantRules) && slices.Equal(gotRoutes, wantRoutes) && len(k.snapshot()) == len(wantRoutes)
	}
	var err error
	switch stop {
	case "ranet3 down":
		r.SetEnabled(false)
		waitFor(t, func() bool { return leftAsKilled() && r.Stats().Err != "" })
		stats := r.Stats()
		if stats.Held != 1 || !strings.Contains(stats.Err, "left the "+string(failing.Family)+" routes and rules in place") || !strings.Contains(stats.Err, "netlink says no") {
			t.Errorf("the down's record reads %d held and %q, want 1 held and the %s routes and rules left", stats.Held, stats.Err, failing.Family)
		}
		k.mu.Lock()
		delete(k.failAddRule, failing)
		k.mu.Unlock()
		waitFor(t, func() bool {
			return k.holds(downRules(downOverlay)) && len(k.snapshot()) == 0 && r.Stats().Err == "" && r.Stats().Held == 2
		})
		return
	case "a clean stop":
		cancel(nil)
	default:
		cancel(ErrMeshFailed)
	}
	select {
	case err = <-done:
		done <- err
	case <-time.After(waitBudget):
		t.Fatal("run did not return after its cancel")
	}
	if err == nil || !strings.Contains(err.Error(), "left the "+string(failing.Family)+" routes and rules in place") {
		t.Errorf("the stop returned %v, want the %s routes and rules left", err, failing.Family)
	}
	if !leftAsKilled() {
		t.Errorf("the stop left %v and %v, want the %s routes and rules as the last pass left them and the other family's down set", k.rules, k.snapshot(), failing.Family)
	}
}

// A reload while a down stopped the reconciler that changes the overlay writes the down set of the new overlay, its adds ahead of
// the deletes of the old one's
func TestReloadWhileDownRewritesTheDownSet(t *testing.T) {
	r, k := downHarness(t)
	runDown(t, r, k)
	r.SetEnabled(false)
	waitFor(t, func() bool { return k.holds(downRules(downOverlay)) })
	moved := prefixes(prefix("192.0.2.0/24"), prefix("3fff::/20"))
	from := len(k.taken())
	if err := r.SetTable(r.Table(), moved, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return k.holds(downRules(moved)) })
	writes := k.taken()[from:]
	lastAdd := slices.IndexFunc(writes, func(w kernelWrite) bool { return w.op != "add rule" })
	if lastAdd != len(downRules(moved)) {
		t.Errorf("the reload wrote %+v, want the new down set's adds ahead of every delete", writes)
	}
	if held := r.Stats().Held; held != 2 {
		t.Errorf("after the reload the record counts %d overlay prefixes held, want 2", held)
	}
}

// A pass that could not list the rules leaves the count of the down set's prefixes as the last pass that listed them found it
func TestPassThatCannotListTheRulesKeepsTheHeldCount(t *testing.T) {
	r, k := downHarness(t)
	for _, rule := range downRules(downOverlay) {
		k.rules[rule] = true
	}
	k.failAddRule[downOverlayRule] = errors.New("netlink says no")
	if err := r.reconcile(); err == nil {
		t.Fatal("the pass reported no error")
	}
	k.failRules = errors.New("netlink says no")
	if err := r.reconcile(); err == nil {
		t.Fatal("the pass that could not list the rules reported no error")
	}
	if held := r.Stats().Held; held != 2 {
		t.Errorf("the pass that could not list the rules counts %d overlay prefixes held, want 2", held)
	}
}

// A stop deletes the down set a stop left for a prefix the overlay no longer holds, which no pass listed since, with its other
// rules, so it leaves the down set of its overlay alone
func TestStopDeletesTheDownSetOfAPrefixTheOverlayNoLongerHolds(t *testing.T) {
	r, k := downHarness(t)
	cancel, wait := runDown(t, r, k)
	k.mu.Lock()
	for _, rule := range downRules(prefixes(prefix("198.51.100.0/24"), prefix("192.0.2.0/24"))) {
		k.rules[rule] = true
	}
	k.mu.Unlock()
	cancel(nil)
	wait()
	if !k.holds(downRules(downOverlay)) {
		t.Errorf("the stop left %v, want the down set of %v alone", k.rules, downOverlay)
	}
}

// A route at an overlay prefix for some sources alone does not hold the prefix for the rest, so a start keeps the down set until a
// route for every source stands there
func TestOnlyARouteForEverySourceHoldsAnOverlayPrefix(t *testing.T) {
	r, _ := downHarness(t)
	down := downRules(downOverlay)
	r.installed = map[Route]bool{}
	for _, p := range downOverlay {
		r.installed[Route{Destination: p.Prefix, Source: prefix("198.51.100.0/24")}] = true
	}
	if r.holdsOverlay(down) {
		t.Errorf("routes from one source at each overlay prefix hold the overlay")
	}
	for _, p := range downOverlay {
		r.installed[Route{Destination: p.Prefix, Unreachable: true}] = true
	}
	if !r.holdsOverlay(down) {
		t.Errorf("a route for every source at each overlay prefix does not hold the overlay")
	}
}
