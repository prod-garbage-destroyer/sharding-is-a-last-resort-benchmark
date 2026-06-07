package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func runCmd(r *runner, name string, args []string, stdin io.Reader) error {
	start := time.Now()
	cmd := exec.CommandContext(context.Background(), name, args...)
	if stdin != nil {
		cmd.Stdin = stdin
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	entry := map[string]any{
		"ts":         time.Now().UTC().Format(time.RFC3339Nano),
		"command":    append([]string{name}, args...),
		"stdout":     trimLog(stdout.String()),
		"stderr":     trimLog(stderr.String()),
		"elapsed_ms": time.Since(start).Milliseconds(),
	}
	r.logBuf.WriteString(jsonString(entry))
	r.logBuf.WriteByte('\n')
	if err != nil {
		return fmt.Errorf("%s %s: %w (%s)", name, strings.Join(args, " "), err, trimLog(stderr.String()))
	}
	return nil
}

func trimLog(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 3000 {
		return s[:3000] + "...<truncated>"
	}
	return s
}

func hostOSVersion() string {
	out, err := exec.Command("sw_vers", "-productVersion").Output()
	if err == nil {
		return strings.TrimSpace(string(out))
	}
	out, err = exec.Command("uname", "-r").Output()
	if err == nil {
		return strings.TrimSpace(string(out))
	}
	return "unknown"
}

func hostCPU() string {
	out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
	if err == nil {
		return strings.TrimSpace(string(out))
	}
	return runtimeArchFallback()
}

func runtimeArchFallback() string {
	return os.Getenv("PROCESSOR_IDENTIFIER")
}

func hostRAMGB() float64 {
	out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 0
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	return v / (1024 * 1024 * 1024)
}

func dockerVersion() *string {
	out, err := exec.Command("docker", "--version").Output()
	if err != nil {
		return nil
	}
	s := strings.TrimSpace(string(out))
	return &s
}

func hostName() string {
	name, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return name
}

func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func imageSizeBytes(image string) int64 {
	out, err := exec.Command("docker", "image", "inspect", image, "--format", "{{.Size}}").Output()
	if err != nil {
		return 0
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func sampleContainer(name string) (ResourceSample, error) {
	out, err := exec.Command("docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}", name).CombinedOutput()
	if err != nil {
		return ResourceSample{}, fmt.Errorf("docker stats %s: %w (%s)", name, err, trimLog(string(out)))
	}
	line := strings.TrimSpace(string(out))
	parts := strings.Split(line, "|")
	if len(parts) != 3 {
		return ResourceSample{}, fmt.Errorf("unexpected stats format for %s: %q", name, line)
	}
	cpu, _ := parsePercent(parts[0])
	memParts := strings.Split(parts[1], "/")
	rss, _ := parseBytes(strings.TrimSpace(memParts[0]))
	netParts := strings.Split(parts[2], "/")
	rx, _ := parseBytes(strings.TrimSpace(netParts[0]))
	tx, _ := parseBytes(strings.TrimSpace(netParts[1]))
	return ResourceSample{CPUPercent: cpu, RSSMB: rss, NetIOMB: rx + tx}, nil
}

func mustSampleTargetContainers(t DBTarget) ResourceSample {
	names := t.Containers()
	if len(names) == 0 {
		return ResourceSample{}
	}
	total := ResourceSample{}
	for _, name := range names {
		s, err := sampleContainer(name)
		if err != nil {
			continue
		}
		total.CPUPercent += s.CPUPercent
		total.RSSMB += s.RSSMB
		total.NetIOMB += s.NetIOMB
	}
	return total
}

func writeVisualDatasets(br BenchmarkResults) error {
	targetOrder := []string{targetManual, targetCockroach, targetTiDB}
	ranking := VisualDataset{Charts: []ChartDef{{
		Type:          "bar",
		Title:         "Throughput ranking",
		XLabel:        strPtr("Target"),
		YLabel:        "ops/s",
		LowerIsBetter: false,
		Series:        chartSeriesFromTargets(br, targetOrder, func(t MetricFamilyRow) float64 { return t.Mean }, throughputColors()),
	}}}
	scaling := VisualDataset{Charts: []ChartDef{{
		Type:          "line",
		Title:         "Throughput scaling by concurrency",
		XLabel:        strPtr("Concurrency"),
		YLabel:        "ops/s",
		LowerIsBetter: false,
		Series: chartSeriesFromConcurrency(br, targetOrder, func(runs []RunResult) float64 {
			return meanRunMetric(runs, func(rr RunResult) float64 { return rr.ThroughputRPS })
		}),
	}}}
	distribution := VisualDataset{Charts: []ChartDef{{
		Type:          "scatter",
		Title:         "Throughput vs tail latency",
		XLabel:        strPtr("p95 latency ms"),
		YLabel:        "ops/s",
		LowerIsBetter: false,
		Series:        scatterSeriesFromTargets(br, targetOrder),
	}}}
	tradeoff := VisualDataset{Charts: []ChartDef{{
		Type:          "horizontal-bar",
		Title:         "Error-rate tradeoff",
		XLabel:        strPtr("Target"),
		YLabel:        "error rate %",
		LowerIsBetter: true,
		Series:        errorSeries(br, targetOrder),
	}}}
	heatmap := VisualDataset{Charts: []ChartDef{{
		Type:          "bar",
		Title:         "Resource footprint",
		XLabel:        strPtr("Target"),
		YLabel:        "RSS MB",
		LowerIsBetter: true,
		Series:        resourceSeries(br, targetOrder),
	}}}
	methodology := VisualDataset{Charts: []ChartDef{{
		Type:          "bar",
		Title:         "Methodology",
		XLabel:        strPtr("Phase"),
		YLabel:        "seconds",
		LowerIsBetter: false,
		Series: []ChartSeries{
			{Target: "warmup", Value: float64(br.WarmupRuns * 10), ColorHint: "cyan"},
			{Target: "measurement", Value: float64(len(br.LoadProfile.ConcurrencyLevels) * br.MeasurementRuns * int(br.LoadProfile.DurationSeconds)), ColorHint: "amber"},
		},
	}}}
	env := VisualDataset{Charts: []ChartDef{{
		Type:          "bar",
		Title:         "Environment summary",
		XLabel:        strPtr("Metric"),
		YLabel:        "value",
		LowerIsBetter: false,
		Series:        environmentSeries(),
	}}}
	matrix := VisualDataset{Charts: []ChartDef{{
		Type:          "scatter",
		Title:         "Throughput / latency matrix",
		XLabel:        strPtr("p95 latency ms"),
		YLabel:        "throughput ops/s",
		LowerIsBetter: false,
		Series:        scatterSeriesFromTargets(br, targetOrder),
	}}}
	files := map[string]VisualDataset{
		"ranking.json":      ranking,
		"scaling.json":      scaling,
		"distribution.json": distribution,
		"tradeoff.json":     tradeoff,
		"heatmap.json":      heatmap,
		"methodology.json":  methodology,
		"environment.json":  env,
		"matrix.json":       matrix,
	}
	for name, payload := range files {
		if err := writeJSON(filepath.Join(brPath(), "visual-datasets", name), payload); err != nil {
			return err
		}
	}
	return writeJSON(filepath.Join(brPath(), "artifacts", "charts-data.json"), map[string]any{"charts": append(append(append([]ChartDef{}, ranking.Charts...), scaling.Charts...), distribution.Charts...)})
}

func brPath() string {
	// The benchmark runner writes outputs relative to the output directory.
	// The current working directory is the runner directory.
	return filepath.Clean(filepath.Join(".", ".."))
}

func chartSeriesFromTargets(br BenchmarkResults, order []string, pick func(MetricFamilyRow) float64, palette []string) []ChartSeries {
	series := []ChartSeries{}
	metric := br.Metrics[0]
	lookup := map[string]MetricFamilyRow{}
	for _, row := range metric.Targets {
		lookup[row.Name] = row
	}
	for idx, name := range order {
		row := lookup[name]
		series = append(series, ChartSeries{Target: name, Value: pick(row), ColorHint: palette[idx%len(palette)]})
	}
	return series
}

func chartSeriesFromRuns(br BenchmarkResults, order []string, pick func(RunResult) float64) []ChartSeries {
	series := []ChartSeries{}
	for idx, target := range order {
		for _, run := range br.RawRuns {
			if run.TargetName != target {
				continue
			}
			series = append(series, ChartSeries{Target: fmt.Sprintf("%s-%d", target, run.RunIndex), Value: pick(run), ColorHint: []string{"cyan", "amber", "red"}[idx%3]})
		}
	}
	return series
}

func chartSeriesFromConcurrency(br BenchmarkResults, order []string, pick func([]RunResult) float64) []ChartSeries {
	series := []ChartSeries{}
	for idx, target := range order {
		for _, concurrency := range br.LoadProfile.ConcurrencyLevels {
			runs := []RunResult{}
			for _, run := range br.RawRuns {
				if run.TargetName == target && run.Concurrency == concurrency {
					runs = append(runs, run)
				}
			}
			series = append(series, ChartSeries{
				Target:    fmt.Sprintf("%s-c%d", target, concurrency),
				Value:     pick(runs),
				ColorHint: []string{"cyan", "amber", "red"}[idx%3],
			})
		}
	}
	return series
}

func meanRunMetric(runs []RunResult, pick func(RunResult) float64) float64 {
	values := []float64{}
	for _, run := range runs {
		values = append(values, pick(run))
	}
	return summarize(values).Mean
}

func scatterSeriesFromTargets(br BenchmarkResults, order []string) []ChartSeries {
	series := []ChartSeries{}
	lookupThroughput := map[string]MetricFamilyRow{}
	lookupLatency := map[string]MetricFamilyRow{}
	for _, metric := range br.Metrics {
		if metric.Name == "throughput" {
			for _, row := range metric.Targets {
				lookupThroughput[row.Name] = row
			}
		}
		if metric.Name == "p95_latency_ms" {
			for _, row := range metric.Targets {
				lookupLatency[row.Name] = row
			}
		}
	}
	for idx, name := range order {
		series = append(series, ChartSeries{Target: name, Value: lookupThroughput[name].Mean, ColorHint: []string{"cyan", "amber", "red"}[idx%3]})
		series = append(series, ChartSeries{Target: name + "-p95", Value: lookupLatency[name].Mean, ColorHint: []string{"cyan", "amber", "red"}[idx%3]})
	}
	return series
}

func errorSeries(br BenchmarkResults, order []string) []ChartSeries {
	series := []ChartSeries{}
	for idx, metric := range br.Metrics {
		if metric.Name != "error_rate_pct" {
			continue
		}
		for _, row := range metric.Targets {
			series = append(series, ChartSeries{Target: row.Name, Value: row.Mean, ColorHint: []string{"green", "amber", "red"}[idx%3]})
		}
	}
	return series
}

func resourceSeries(br BenchmarkResults, order []string) []ChartSeries {
	series := []ChartSeries{}
	for idx, run := range br.RawRuns {
		series = append(series, ChartSeries{Target: fmt.Sprintf("%s-%d", run.TargetName, run.RunIndex), Value: run.ResourceSample.RSSMB, ColorHint: []string{"cyan", "amber", "red"}[idx%3]})
	}
	return series
}

func environmentSeries() []ChartSeries {
	return []ChartSeries{
		{Target: "docker", Value: 1, ColorHint: "cyan"},
		{Target: "go", Value: 1, ColorHint: "amber"},
	}
}

func throughputColors() []string { return []string{"cyan", "amber", "red"} }

func (r *runner) writeVisualDatasets(br BenchmarkResults) error {
	return writeVisualDatasets(br)
}

func (r *runner) startManualPostgres() error {
	_ = runCmd(r, "docker", []string{"rm", "-f", "manual-pg-0", "manual-pg-1", "manual-pg-2"}, nil)
	ports := []string{"55432:5432", "55433:5432", "55434:5432"}
	for i := 0; i < 3; i++ {
		args := []string{
			"run", "-d",
			"--name", fmt.Sprintf("manual-pg-%d", i),
			"--network", "sharding-bench-net",
			"--cpus", "1",
			"--memory", "512m",
			"-e", "POSTGRES_USER=benchmark",
			"-e", "POSTGRES_PASSWORD=benchmark",
			"-e", "POSTGRES_DB=benchmark",
			"-p", ports[i],
			"postgres:16-alpine",
		}
		if err := runCmd(r, "docker", args, nil); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) startCockroach() error {
	_ = runCmd(r, "docker", []string{"rm", "-f", "roach1", "roach2", "roach3"}, nil)
	shared := []string{"--network", "sharding-bench-net", "--cpus", "1", "--memory", "2g"}
	nodes := []struct {
		name     string
		port     string
		httpPort string
		join     string
	}{
		{"roach1", "26257:26257", "8081:8080", "roach1:26257,roach2:26257,roach3:26257"},
		{"roach2", "", "", "roach1:26257,roach2:26257,roach3:26257"},
		{"roach3", "", "", "roach1:26257,roach2:26257,roach3:26257"},
	}
	for idx, node := range nodes {
		args := []string{"run", "-d", "--name", node.name}
		args = append(args, shared...)
		if node.port != "" {
			args = append(args, "-p", node.port)
		}
		if node.httpPort != "" {
			args = append(args, "-p", node.httpPort)
		}
		args = append(args,
			"cockroachdb/cockroach:v25.3.4",
			"start", "--insecure",
			"--listen-addr=0.0.0.0:26257",
			"--http-addr=0.0.0.0:8080",
			fmt.Sprintf("--join=%s", node.join),
			fmt.Sprintf("--advertise-addr=%s:26257", node.name),
			"--cache=.25",
			"--max-sql-memory=.25",
		)
		if idx > 0 {
			args = append(args, "--store=path=/cockroach/cockroach-data")
		}
		if err := runCmd(r, "docker", args, nil); err != nil {
			return err
		}
	}
	time.Sleep(8 * time.Second)
	if err := runCmd(r, "docker", []string{"exec", "roach1", "cockroach", "init", "--insecure"}, nil); err != nil {
		// init is idempotent; ignore "already initialized" by continuing on stderr.
		if !strings.Contains(err.Error(), "already initialized") {
			return err
		}
	}
	return nil
}

func (r *runner) startTiDB() error {
	_ = runCmd(r, "docker", []string{"rm", "-f", "pd0", "pd1", "pd2", "tikv0", "tikv1", "tikv2", "tidb"}, nil)
	runtimeRoot := filepath.Join(r.cfg.OutputDir, ".tidb-runtime")
	dataRoot := filepath.Join(runtimeRoot, "data")
	logRoot := filepath.Join(runtimeRoot, "logs")
	configRoot := filepath.Join(runtimeRoot, "config")
	_ = os.RemoveAll(runtimeRoot)
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(logRoot, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(configRoot, 0o755); err != nil {
		return err
	}
	if err := copyTiDBConfigs(configRoot); err != nil {
		return err
	}
	pdNodes := []string{"pd0", "pd1", "pd2"}
	for _, node := range pdNodes {
		args := []string{
			"run", "-d",
			"--name", node,
			"--network", "sharding-bench-net",
			"--cpus", "1",
			"--memory", "512m",
			"-v", filepath.Join(configRoot, "pd.toml") + ":/pd.toml:ro",
			"-v", dataRoot + ":/data",
			"-v", logRoot + ":/logs",
			"pingcap/pd:latest",
			"--name=" + node,
			"--client-urls=http://0.0.0.0:2379",
			"--peer-urls=http://0.0.0.0:2380",
			"--advertise-client-urls=http://" + node + ":2379",
			"--advertise-peer-urls=http://" + node + ":2380",
			"--initial-cluster=pd0=http://pd0:2380,pd1=http://pd1:2380,pd2=http://pd2:2380",
			"--data-dir=/data/" + node,
			"--config=/pd.toml",
			"--log-file=/logs/" + node + ".log",
		}
		if err := runCmd(r, "docker", args, nil); err != nil {
			return err
		}
	}
	time.Sleep(6 * time.Second)
	for _, node := range []string{"tikv0", "tikv1", "tikv2"} {
		args := []string{
			"run", "-d",
			"--name", node,
			"--network", "sharding-bench-net",
			"--cpus", "1",
			"--memory", "2g",
			"-v", filepath.Join(configRoot, "tikv.toml") + ":/tikv.toml:ro",
			"-v", dataRoot + ":/data",
			"-v", logRoot + ":/logs",
			"pingcap/tikv:latest",
			"--addr=0.0.0.0:20160",
			"--advertise-addr=" + node + ":20160",
			"--data-dir=/data/" + node,
			"--pd=pd0:2379,pd1:2379,pd2:2379",
			"--config=/tikv.toml",
			"--log-file=/logs/" + node + ".log",
		}
		if err := runCmd(r, "docker", args, nil); err != nil {
			return err
		}
	}
	time.Sleep(10 * time.Second)
	if err := runCmd(r, "docker", []string{
		"run", "-d",
		"--name", "tidb",
		"--network", "sharding-bench-net",
		"--cpus", "1",
		"--memory", "2g",
		"-p", "4000:4000",
		"-p", "10080:10080",
		"-v", filepath.Join(configRoot, "tidb.toml") + ":/tidb.toml:ro",
		"-v", logRoot + ":/logs",
		"pingcap/tidb:latest",
		"--store=tikv",
		"--path=pd0:2379,pd1:2379,pd2:2379",
		"--config=/tidb.toml",
		"--log-file=/logs/tidb.log",
		"--advertise-address=tidb",
	}, nil); err != nil {
		return err
	}
	return nil
}

func copyTiDBConfigs(dst string) error {
	srcRoot := filepath.Join("/private/tmp", "tidb-docker-compose", "config")
	for _, name := range []string{"pd.toml", "tikv.toml", "tidb.toml"} {
		data, err := os.ReadFile(filepath.Join(srcRoot, name))
		if err != nil {
			data = []byte("# generated minimal config for local benchmark\n")
		}
		if err := os.WriteFile(filepath.Join(dst, name), data, 0o644); err != nil {
			return err
		}
	}
	return nil
}
