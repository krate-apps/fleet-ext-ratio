package main

import "testing"

func TestMatchReasonsRatio(t *testing.T) {
	g := group{Name: "tv", MaxRatio: 2.0, Action: "stop"}
	trow := torrentRow{Complete: true, Ratio: 2.1, Label: "tv"}
	ok, reason := matchReasons(trow, g, 1000)
	if !ok || reason == "" {
		t.Fatalf("expected ratio match got %v %q", ok, reason)
	}
}

func TestMatchReasonsSeedTime(t *testing.T) {
	g := group{Name: "tv", MaxSeedSeconds: 3600, Action: "pause"}
	trow := torrentRow{Complete: true, FinishedAt: 100, SeedingSeconds: 4000}
	ok, reason := matchReasons(trow, g, 5000)
	if !ok {
		t.Fatalf("expected seed match: %q", reason)
	}
}

func TestMatchReasonsFinishedAtFallback(t *testing.T) {
	g := group{Name: "tv", MaxSeedSeconds: 100, Action: "stop"}
	trow := torrentRow{Complete: true, FinishedAt: 1000}
	ok, _ := matchReasons(trow, g, 1200)
	if !ok {
		t.Fatal("expected finishedAt-derived seed age")
	}
}

func TestMatchReasonsIncomplete(t *testing.T) {
	g := group{Name: "tv", MaxRatio: 1, Action: "stop"}
	ok, _ := matchReasons(torrentRow{Complete: false, Ratio: 9}, g, 1)
	if ok {
		t.Fatal("incomplete must not match")
	}
}

func TestMatchReasonsMinUploadBlocks(t *testing.T) {
	g := group{Name: "tv", MaxRatio: 1, MinUploadBytes: 1000, Action: "stop"}
	ok, _ := matchReasons(torrentRow{Complete: true, Ratio: 2, UpTotal: 10}, g, 1)
	if ok {
		t.Fatal("min upload should block")
	}
}

func TestValidateConfigSeedOnly(t *testing.T) {
	cfg := config{Groups: []group{{Name: "x", MaxSeedSeconds: 60, Action: "stop"}}}
	if err := validateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Groups[0].MaxSeedSeconds = 0
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected error without maxRatio/maxSeedSeconds")
	}
}
