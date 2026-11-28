// SPDX-FileCopyrightText: 2026 Yifei Sun
// SPDX-License-Identifier: FSL-1.1-ALv2

package client

import (
	"os"
	"path/filepath"
	"testing"

	"ranet3.com/pkgs/ranet3/internal/config"
)

func TestSourcedShippedExamplesRemainInactive(t *testing.T) {
	dir := filepath.Join("..", "..", "examples")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch filepath.Ext(name) {
		case ".toml", ".yaml", ".yml", ".json":
		default:
			continue
		}
		t.Run(name, func(t *testing.T) {
			cfg, err := config.Load(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Cap.Table != nil {
				t.Fatalf("shipped example starts the kernel reconciler: %+v", cfg.Cap.Table)
			}
			if len(cfg.Segments().Steer) != 0 {
				t.Fatal("shipped example activates segment steering")
			}
			if err := refuseExitModeHere(cfg); err != nil {
				t.Fatalf("shipped example refused on this platform: %v", err)
			}
		})
	}
}
