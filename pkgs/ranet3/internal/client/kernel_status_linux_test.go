// SPDX-FileCopyrightText: 2026 Yifei Sun
// SPDX-License-Identifier: FSL-1.1-ALv2

//go:build linux && !android

package client

import (
	"context"
	"os"
	"testing"
	"time"

	"ranet3.com/pkgs/ranet3/internal/kernel"
	"ranet3.com/pkgs/ranet3/internal/netnstest"
	"ranet3.com/pkgs/ranet3/internal/netstack"
	"ranet3.com/pkgs/ranet3/schema"
)

const (
	// statusBudget bounds how long the test waits for the reconciler's own record to show a withdrawal
	statusBudget = 10 * time.Second
	// statusPoll is how often the test reads that record while it waits
	statusPoll = 10 * time.Millisecond
)

// ReconcilerOf hands the node a reconciler whose status counts the overlay prefixes a down's stop refuses, as the reconciler's own
// record does, and whose DropHold has the operator's stop that follows take them, the status counting 0 after it
func TestReconcilerOfCountsTheDownAndDropsItForTheOperatorsStop(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("the reconciler's netlink writes need root on linux")
	}
	ns := netnstest.StartThread(t)
	overlay := []schema.Prefix{schema.MustPrefix("192.0.2.0/25"), schema.MustPrefix("3fff::/21")}
	var routes *kernel.Reconciler
	if err := ns.Do(func() (err error) {
		routes, err = kernel.New(kernel.Table{ID: kernel.DefaultTable, Proto: kernel.DefaultProtocol}.Normalized(), kernel.Runtime{Interface: "lo", Overlay: overlay}, netstack.NewRouteTable())
		return err
	}); err != nil {
		t.Fatalf("build the reconciler: %v", err)
	}
	r := ReconcilerOf(routes)
	// a down's stop, before the first pass
	r.Enable(false)
	ctx, cancel := context.WithCancel(context.Background())
	runner := netnstest.JoinThread(t, ns.Path)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Do(func() error { return routes.Run(ctx) })
	}()
	t.Cleanup(func() { r.DropHold(); cancel(); <-done })
	settled := func(held int) bool {
		for deadline := time.Now().Add(statusBudget); time.Now().Before(deadline); time.Sleep(statusPoll) {
			if stats := routes.Stats(); !stats.At.IsZero() && stats.Held == held {
				return true
			}
		}
		return false
	}
	if !settled(2) {
		t.Fatalf("the reconciler's own record never counted the 2 overlay prefixes of the down set: %+v", routes.Stats())
	}
	if got := r.Status().Held; got != 2 {
		t.Errorf("the down's status counts %d overlay prefixes refused, want the reconciler's 2", got)
	}
	r.DropHold()
	r.Enable(false)
	if !settled(0) {
		t.Errorf("after DropHold the operator's stop left the reconciler counting %d overlay prefixes refused, want 0", routes.Stats().Held)
	}
	if got := r.Status().Held; got != 0 {
		t.Errorf("the operator's stop's status counts %d overlay prefixes refused, want 0", got)
	}
	var dumped []kernel.DumpedRule
	if err := ns.Do(func() (err error) { dumped, err = kernel.DumpRules(); return err }); err != nil {
		t.Fatal(err)
	}
	if extra, _ := kernel.NotDownSet(dumped, nil, kernel.DefaultProtocol); len(extra) != 0 {
		t.Errorf("the operator's stop left the rules %+v", extra)
	}
}
