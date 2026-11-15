// Command ratio-worker enforces ratio groups via Fleet public SDK (labels + actions).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultIntervalSec = 60
	minIntervalSec     = 15
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type group struct {
	Name           string  `json:"name"`
	Enabled        bool    `json:"enabled"`
	MinRatio       float64 `json:"minRatio"`
	MaxRatio       float64 `json:"maxRatio"`
	MinUploadBytes int64   `json:"minUploadBytes"`
	MaxSeedSeconds int64   `json:"maxSeedSeconds,omitempty"` // 0 = disabled
	Action         string  `json:"action"`                     // stop|pause|erase
}

type config struct {
	Enabled     bool    `json:"enabled"`
	IntervalSec int     `json:"intervalSec,omitempty"`
	Groups      []group `json:"groups"`
}

type torrentRow struct {
	Hash           string  `json:"hash"`
	Name           string  `json:"name"`
	Ratio          float64 `json:"ratio"`
	Label          string  `json:"label"`
	State          string  `json:"state"`
	UpTotal        int64   `json:"upTotal"`
	Complete       bool    `json:"complete"`
	SeedingSeconds int64   `json:"seedingSeconds"`
	FinishedAt     int64   `json:"finishedAt"`
}

type matchHit struct {
	Hash   string `json:"hash"`
	Name   string `json:"name"`
	Label  string `json:"label"`
	Group  string `json:"group"`
	Reason string `json:"reason"`
	Action string `json:"action"`
	Ratio  float64 `json:"ratio,omitempty"`
	SeedSec int64  `json:"seedingSeconds,omitempty"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	go loopEvaluate()

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		go handleRequest(req)
	}
}

func handleRequest(req request) {
	switch req.Method {
	case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
		reply(req.ID, map[string]any{"ok": true})
	case "fleet.shutdown":
		reply(req.ID, map[string]any{"ok": true})
		os.Exit(0)
	case "ext.ratio.get":
		cfg, err := loadConfig()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, cfg)
	case "ext.ratio.save":
		var cfg config
		if err := json.Unmarshal(req.Params, &cfg); err != nil {
			replyErr(req.ID, "invalid config")
			return
		}
		if err := validateConfig(cfg); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		normalizeConfig(&cfg)
		if err := saveConfig(&cfg); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, cfg)
	case "ext.ratio.evaluate":
		res, err := evaluate(false)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	case "ext.ratio.preview":
		res, err := evaluate(true)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	default:
		replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func loopEvaluate() {
	time.Sleep(5 * time.Second)
	for {
		cfg, err := loadConfig()
		interval := defaultIntervalSec
		if err == nil && cfg.IntervalSec > 0 {
			interval = cfg.IntervalSec
		}
		if err == nil && cfg.Enabled {
			_, _ = evaluate(false)
		}
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func normalizeConfig(cfg *config) {
	if cfg.IntervalSec <= 0 {
		cfg.IntervalSec = defaultIntervalSec
	}
	if cfg.IntervalSec < minIntervalSec {
		cfg.IntervalSec = minIntervalSec
	}
}

func validateConfig(cfg config) error {
	if cfg.IntervalSec != 0 && cfg.IntervalSec < minIntervalSec {
		return fmt.Errorf("intervalSec must be >= %d", minIntervalSec)
	}
	for i, g := range cfg.Groups {
		name := strings.TrimSpace(g.Name)
		if name == "" {
			return fmt.Errorf("groups[%d]: name required", i)
		}
		if g.MaxRatio <= 0 && g.MaxSeedSeconds <= 0 {
			return fmt.Errorf("groups[%d]: maxRatio or maxSeedSeconds required", i)
		}
		if g.MaxRatio < 0 || g.MinRatio < 0 || g.MinUploadBytes < 0 || g.MaxSeedSeconds < 0 {
			return fmt.Errorf("groups[%d]: min/max values must be >= 0", i)
		}
		switch strings.ToLower(strings.TrimSpace(g.Action)) {
		case "stop", "pause", "erase":
		default:
			return fmt.Errorf("groups[%d]: action must be stop|pause|erase", i)
		}
	}
	return nil
}

func seedingAge(t torrentRow, now int64) int64 {
	if t.SeedingSeconds > 0 {
		return t.SeedingSeconds
	}
	if t.FinishedAt > 0 && now > t.FinishedAt {
		return now - t.FinishedAt
	}
	return 0
}

func matchReasons(t torrentRow, g group, now int64) (bool, string) {
	if !t.Complete {
		return false, ""
	}
	seed := seedingAge(t, now)
	ratioOK := false
	seedOK := false
	reasons := make([]string, 0, 2)

	if g.MaxRatio > 0 && t.Ratio >= g.MaxRatio {
		if g.MinRatio > 0 && t.Ratio < g.MinRatio {
			// max reached but below explicit min floor — treat as not ratio-triggered
		} else if g.MinUploadBytes > 0 && t.UpTotal < g.MinUploadBytes {
			// upload floor not met
		} else {
			ratioOK = true
			reasons = append(reasons, fmt.Sprintf("ratio>=%.3f", g.MaxRatio))
		}
	}
	if g.MaxSeedSeconds > 0 && seed >= g.MaxSeedSeconds {
		seedOK = true
		reasons = append(reasons, fmt.Sprintf("seed>=%ds", g.MaxSeedSeconds))
	}
	if !ratioOK && !seedOK {
		return false, ""
	}
	return true, strings.Join(reasons, "+")
}

func evaluate(dryRun bool) (map[string]any, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return map[string]any{"ok": true, "applied": 0, "matches": []matchHit{}, "reason": "disabled", "dryRun": dryRun}, nil
	}
	raw, err := hostCall("fleet.torrents.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Torrents []torrentRow `json:"torrents"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, err
	}
	byLabel := map[string]group{}
	for _, g := range cfg.Groups {
		if !g.Enabled {
			continue
		}
		byLabel[strings.TrimSpace(g.Name)] = g
	}
	now := time.Now().Unix()
	matches := make([]matchHit, 0)
	applied := 0
	details := []map[string]any{}
	for _, t := range wrap.Torrents {
		g, ok := byLabel[strings.TrimSpace(t.Label)]
		if !ok {
			continue
		}
		hit, reason := matchReasons(t, g, now)
		if !hit {
			continue
		}
		action := strings.ToLower(strings.TrimSpace(g.Action))
		m := matchHit{
			Hash:    t.Hash,
			Name:    t.Name,
			Label:   t.Label,
			Group:   g.Name,
			Reason:  reason,
			Action:  action,
			Ratio:   t.Ratio,
			SeedSec: seedingAge(t, now),
		}
		matches = append(matches, m)
		if dryRun {
			continue
		}
		if _, err := hostCall("fleet.torrents.action", map[string]any{
			"action": action,
			"hashes": []string{t.Hash},
		}); err != nil {
			details = append(details, map[string]any{"hash": t.Hash, "ok": false, "error": err.Error()})
			continue
		}
		applied++
		details = append(details, map[string]any{
			"hash": t.Hash, "ok": true, "action": action, "group": g.Name,
			"ratio": t.Ratio, "reason": reason, "seedingSeconds": m.SeedSec,
		})
	}
	out := map[string]any{
		"ok":      true,
		"dryRun":  dryRun,
		"matches": matches,
		"matched": len(matches),
		"checked": len(wrap.Torrents),
	}
	if dryRun {
		out["applied"] = 0
	} else {
		out["applied"] = applied
		out["details"] = details
	}
	return out, nil
}

func loadConfig() (*config, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "groups"})
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return &config{Enabled: false, IntervalSec: defaultIntervalSec, Groups: []group{}}, nil
	}
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return &config{Enabled: false, IntervalSec: defaultIntervalSec, Groups: []group{}}, nil
	}
	normalizeConfig(&cfg)
	return &cfg, nil
}

func saveConfig(cfg *config) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "groups", "value": cfg})
	return err
}

func hostCall(method string, params any) (json.RawMessage, error) {
	mu.Lock()
	id := nextHost
	nextHost++
	ch := make(chan hostReply, 1)
	pending[id] = ch
	mu.Unlock()
	req := map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": id}
	line, _ := json.Marshal(req)
	line = append(line, '\n')
	outMu.Lock()
	_, err := os.Stdout.Write(line)
	outMu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case rep := <-ch:
		if rep.err != "" {
			return nil, fmt.Errorf("%s", rep.err)
		}
		if len(rep.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return rep.result, nil
	case <-time.After(30 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func replyErr(id any, msg string) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": -32000, "message": msg},
	})
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}
