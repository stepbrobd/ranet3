// SPDX-FileCopyrightText: 2026 Yifei Sun
// SPDX-License-Identifier: FSL-1.1-ALv2

package kernel

import (
	"context"
	"errors"
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
	op   string
	rule Rule
}

// recordingKernel is the fake kernel noting each write it took, in the order taken
type recordingKernel struct {
	*fakeKernel
	noted  sync.Mutex
	writes []kernelWrite
}

func (k *recordingKernel) note(op string, rule Rule) {
	k.noted.Lock()
	defer k.noted.Unlock()
	k.writes = append(k.writes, kernelWrite{op, rule})
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
		k.note("delete route", Rule{})
	}
	return err
}

func (k *recordingKernel) AddRule(rule Rule) error {
	err := k.fakeKernel.AddRule(rule)
	if err == nil {
		k.note("add rule", rule)
	}
	return err
}

func (k *recordingKernel) DelRule(rule Rule) error {
	err := k.fakeKernel.DelRule(rule)
	if err == nil {
		k.note("delete rule", rule)
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

// Each stop but the operator's adds the down set before it deletes any route or rule, and leaves it as the reconciler's only rules,
// counted in the record of a down's withdrawal
func TestStopAddsTheDownSetBeforeAnythingLeaves(t *testing.T) {
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

// A start keeps the down set a stop left until its pass added every rule it derives, 5110 among them, and holds a route at each
// overlay prefix, and deletes at once the rules a stop left for a prefix the overlay no longer holds
func TestStartTakesTheDownSetOnceItsRulesStand(t *testing.T) {
	r, k := downHarness(t)
	down, gone := downRules(downOverlay), downRules(prefixes(prefix("198.51.100.0/24")))
	for _, rule := range slices.Concat(down, gone) {
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
		if w.op == "delete rule" && slices.Contains(down, w.rule) && i < last {
			t.Errorf("the start deleted %s ahead of its last add at %d, %+v", w.rule, priorityOverlay, writes[last])
		}
	}
	if !k.holdsDerived(r) || k.holdsAny(down) || k.holdsAny(gone) {
		t.Errorf("after the start the kernel holds %v, want the derived rules alone", k.rules)
	}
	if held := r.Stats().Held; held != 0 {
		t.Errorf("the start's record counts %d overlay prefixes held, want 0", held)
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

// The warning of a start with no reconciler names each rule of the down set an earlier instance left under its protocol, with the
// command that deletes it, once for them all, and nothing of another shape: a rule at the refusal's priority that looks a table
// up, and a main lookup whose suppress is not the prefix's length less one. With no such rule it says nothing.
func TestStartWithNoReconcilerNamesEachRuleOfTheDownSetLeft(t *testing.T) {
	const proto = 99
	var dumped []DumpedRule
	for _, rule := range downRules(downOverlay) {
		dumped = append(dumped, rule.dumped(proto))
	}
	other := Rule{Family: FamilyIPv4, Priority: priorityDownRefused, To: schema.PrefixFrom(prefix("198.51.100.0/24")), Table: 300}
	wider := Rule{Family: FamilyIPv4, Priority: priorityDownMain, To: schema.PrefixFrom(prefix("203.0.113.0/24")), Table: schema.TableMain, suppress: SuppressUpTo(0)}
	dumped = append(dumped, other.dumped(proto), wider.dumped(proto))
	said := logged(t)
	warnHeldOverlay(dumped)
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
	if strings.Contains(text, "198.51.100.0/24") || strings.Contains(text, "203.0.113.0/24") {
		t.Errorf("the warning names a rule of another shape:\n%s", text)
	}
	said.Reset()
	warnHeldOverlay([]DumpedRule{other.dumped(proto), wider.dumped(proto)})
	if said.Len() != 0 {
		t.Errorf("with no rule of the down set the start warned:\n%s", said.String())
	}
}
