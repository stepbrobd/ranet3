// SPDX-FileCopyrightText: 2026 Yifei Sun
// SPDX-License-Identifier: FSL-1.1-ALv2

//go:build linux || darwin

package kernel

import (
	"net/netip"
	"testing"

	"hegel.dev/go/hegel"

	"ranet3.com/pkgs/ranet3/internal/pbt"
	"ranet3.com/pkgs/ranet3/schema"
)

// downReference is the down set as routing.md states it, rule by rule: for
// each overlay entry longer than /0, masked, in its family, a refusal at 32765
// and a lookup of main at 32764 suppressing every route shorter than the entry
func downReference(overlay []schema.Prefix) map[Rule]bool {
	out := map[Rule]bool{}
	for _, entry := range overlay {
		if !entry.Prefix.IsValid() || entry.Prefix.Bits() == 0 {
			continue
		}
		to := schema.PrefixFrom(entry.Prefix.Masked())
		family := FamilyIPv6
		if to.Addr().Is4() {
			family = FamilyIPv4
		}
		out[Rule{Family: family, Priority: 32765, To: to, action: actionUnreachable}] = true
		out[Rule{Family: family, Priority: 32764, To: to, Table: 254, suppress: SuppressUpTo(to.Bits() - 1)}] = true
	}
	return out
}

// drawnOverlayEntry is an overlay entry: none, or an address of either family
// with its host bits as drawn at a length from /0 to a host prefix
func drawnOverlayEntry(tc hegel.TestCase) schema.Prefix {
	width := 4
	switch hegel.Draw(tc, pbt.Spanning(0, 2)) {
	case 0:
		return schema.Prefix{}
	case 2:
		width = 16
	}
	raw := make([]byte, width)
	for i := range raw {
		raw[i] = byte(hegel.Draw(tc, hegel.Integers(0, 255)))
	}
	address, _ := netip.AddrFromSlice(raw)
	return schema.PrefixFrom(netip.PrefixFrom(address, hegel.Draw(tc, pbt.Spanning(0, 8*width))))
}

// For every overlay the down set holds exactly the reference's rules, each
// once, and each prefix's refusal ahead of its lookup of main, so a stop that
// adds them in order never holds the lookup without the refusal behind it
func TestDownSetIsTwoRulesForEachOverlayPrefix(t *testing.T) {
	pbt.Check(t, func(ht *hegel.T) {
		var overlay []schema.Prefix
		for range hegel.Draw(ht, pbt.Spanning(0, 6)) {
			// an entry written twice is one prefix
			if len(overlay) > 0 && hegel.Draw(ht, pbt.Spanning(0, 3)) == 3 {
				overlay = append(overlay, overlay[hegel.Draw(ht, hegel.Integers(0, len(overlay)-1))])
				continue
			}
			overlay = append(overlay, drawnOverlayEntry(ht))
		}
		got, want := downRules(overlay), downReference(overlay)
		at := map[Rule]int{}
		for i, rule := range got {
			if !want[rule] {
				ht.Fatalf("the down set of %v holds %s, which the reference does not", overlay, rule)
			}
			if _, twice := at[rule]; twice {
				ht.Fatalf("the down set of %v holds %s twice", overlay, rule)
			}
			at[rule] = i
		}
		if len(at) != len(want) {
			ht.Fatalf("the down set of %v is %v, want the %d rules of %v", overlay, got, len(want), want)
		}
		for rule, i := range at {
			if rule.Priority != 32764 {
				continue
			}
			refusal := Rule{Family: rule.Family, Priority: 32765, To: rule.To, action: actionUnreachable}
			if at[refusal] > i {
				ht.Fatalf("the down set of %v holds %s ahead of %s", overlay, rule, refusal)
			}
		}
	})
}
