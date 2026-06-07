package main

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	opIncrement     = "increment"
	opReadBalance   = "read_balance"
	opTransfer      = "transfer"
	opRangeReport   = "range_report"
	targetManual    = "manual-postgres"
	targetCockroach = "cockroachdb"
	targetTiDB      = "tidb"
)

type Config struct {
	OutputDir         string
	WorkDir           string
	Seed              int64
	Concurrency       int
	ConcurrencyLevels []int
	WarmupRuns        int
	WarmupDuration    time.Duration
	MeasurementRuns   int
	MeasurementDur    time.Duration
	Keyspace          int64
	RangeWindow       int64
	TransferAmount    int64
	RequestMix        map[string]int
	ContainerCPULimit string
	ContainerMemLimit string
}

type PhaseRecord struct {
	Name       string   `json:"name"`
	PhaseStart string   `json:"phase_start_ts"`
	PhaseEnd   string   `json:"phase_end_ts"`
	Status     string   `json:"status"`
	Commands   []string `json:"commands"`
	Artifacts  []string `json:"artifacts"`
}

type EnvironmentActionLog struct {
	Phases []PhaseRecord `json:"phases"`
}

type BuildTargetResult struct {
	Name              string   `json:"name"`
	Toolchain         string   `json:"toolchain"`
	Framework         string   `json:"framework"`
	BuildCommand      string   `json:"build_command"`
	ExitCode          int      `json:"exit_code"`
	DurationMs        int64    `json:"duration_ms"`
	BinarySizeBytes   int64    `json:"binary_size_bytes"`
	ImageSizeBytes    int64    `json:"image_size_bytes"`
	DependenciesCount int      `json:"dependencies_count"`
	WarningsCount     int      `json:"warnings_count"`
	Errors            []string `json:"errors"`
	StdoutExcerpt     string   `json:"stdout_excerpt"`
	Notes             string   `json:"notes"`
}

type BuildResults struct {
	Targets []BuildTargetResult `json:"targets"`
}

type TestFailure struct {
	TestName string `json:"test_name"`
	Error    string `json:"error"`
}

type TestTargetResult struct {
	Name        string        `json:"name"`
	TestCommand string        `json:"test_command"`
	ExitCode    int           `json:"exit_code"`
	Total       int           `json:"total"`
	Passed      int           `json:"passed"`
	Failed      int           `json:"failed"`
	Skipped     int           `json:"skipped"`
	DurationMs  int64         `json:"duration_ms"`
	CoveragePct float64       `json:"coverage_pct"`
	Failures    []TestFailure `json:"failures"`
	Notes       string        `json:"notes"`
}

type TestResults struct {
	Targets []TestTargetResult `json:"targets"`
}

type MetricAggregate struct {
	Mean         float64   `json:"mean"`
	Median       float64   `json:"median"`
	P50          float64   `json:"p50"`
	P95          float64   `json:"p95"`
	P99          float64   `json:"p99"`
	Min          float64   `json:"min"`
	Max          float64   `json:"max"`
	Stddev       float64   `json:"stddev"`
	RunValues    []float64 `json:"run_values"`
	ErrorRatePct float64   `json:"error_rate_pct"`
}

type RunOperationStats struct {
	Count     int64     `json:"count"`
	Errors    int64     `json:"errors"`
	Latencies []float64 `json:"latencies_ms"`
}

type RunResult struct {
	TargetName      string                       `json:"target_name"`
	RunIndex        int                          `json:"run_index"`
	Concurrency     int                          `json:"concurrency"`
	DurationSeconds float64                      `json:"duration_seconds"`
	RequestsTotal   int64                        `json:"requests_total"`
	SuccessCount    int64                        `json:"success_count"`
	ErrorCount      int64                        `json:"error_count"`
	ThroughputRPS   float64                      `json:"throughput_rps"`
	OperationStats  map[string]RunOperationStats `json:"operation_stats"`
	Latency         MetricAggregate              `json:"latency"`
	ResourceSample  ResourceSample               `json:"resource_sample"`
	Notes           string                       `json:"notes"`
}

type ResourceSample struct {
	CPUPercent float64 `json:"cpu_percent"`
	RSSMB      float64 `json:"rss_mb"`
	NetIOMB    float64 `json:"net_io_mb"`
}

type TargetAggregate struct {
	Name         string    `json:"name"`
	RunValues    []float64 `json:"run_values"`
	Mean         float64   `json:"mean"`
	Median       float64   `json:"median"`
	P50          float64   `json:"p50"`
	P95          float64   `json:"p95"`
	P99          float64   `json:"p99"`
	Min          float64   `json:"min"`
	Max          float64   `json:"max"`
	Stddev       float64   `json:"stddev"`
	ErrorRatePct float64   `json:"error_rate_pct"`
	Notes        string    `json:"notes"`
}

type BenchmarkResults struct {
	ExperimentType   string                     `json:"experiment_type"`
	BenchmarkTool    string                     `json:"benchmark_tool"`
	BenchmarkVersion string                     `json:"benchmark_tool_version"`
	WarmupRuns       int                        `json:"warmup_runs"`
	MeasurementRuns  int                        `json:"measurement_runs"`
	LoadProfile      LoadProfile                `json:"load_profile"`
	Metrics          []BenchmarkMetricFamily    `json:"metrics"`
	RawLogs          []string                   `json:"raw_logs"`
	Targets          map[string]TargetAggregate `json:"targets"`
	RawRuns          []RunResult                `json:"raw_runs"`
}

type LoadProfile struct {
	Concurrency       int    `json:"concurrency"`
	ConcurrencyLevels []int  `json:"concurrency_levels"`
	DurationSeconds   int    `json:"duration_seconds"`
	RequestMix        string `json:"request_mix"`
}

type BenchmarkMetricFamily struct {
	Name          string            `json:"name"`
	Unit          string            `json:"unit"`
	LowerIsBetter bool              `json:"lower_is_better"`
	Targets       []MetricFamilyRow `json:"targets"`
}

type MetricFamilyRow struct {
	Name         string    `json:"name"`
	RunValues    []float64 `json:"run_values"`
	Mean         float64   `json:"mean"`
	Median       float64   `json:"median"`
	P50          float64   `json:"p50"`
	P95          float64   `json:"p95"`
	P99          float64   `json:"p99"`
	Min          float64   `json:"min"`
	Max          float64   `json:"max"`
	Stddev       float64   `json:"stddev"`
	ErrorRatePct float64   `json:"error_rate_pct"`
	Notes        string    `json:"notes"`
}

type VisualDataset struct {
	Charts []ChartDef `json:"charts"`
}

type ChartDef struct {
	Type          string            `json:"type"`
	Title         string            `json:"title"`
	XLabel        *string           `json:"x_label"`
	YLabel        string            `json:"y_label"`
	LowerIsBetter bool              `json:"lower_is_better"`
	Annotations   []ChartAnnotation `json:"annotations"`
	Series        []ChartSeries     `json:"series"`
}

type ChartAnnotation struct {
	Target string  `json:"target"`
	Value  float64 `json:"value"`
	Label  string  `json:"label"`
}

type ChartSeries struct {
	Target    string  `json:"target"`
	Value     float64 `json:"value"`
	ColorHint string  `json:"color_hint"`
}

type Verdict struct {
	ExperimentSlug     string           `json:"experiment_slug"`
	Topic              string           `json:"topic"`
	Targets            []string         `json:"targets"`
	ExperimentType     string           `json:"experiment_type"`
	OverallFraming     string           `json:"overall_framing"`
	CategoryWinners    []CategoryWinner `json:"category_winners"`
	PerTarget          []TargetVerdict  `json:"per_target"`
	Confidence         string           `json:"confidence"`
	ConfidenceNotes    string           `json:"confidence_notes"`
	Caveats            []string         `json:"caveats"`
	FairnessViolations []string         `json:"fairness_violations"`
	FailedTargets      []string         `json:"failed_targets"`
	VerdictValid       bool             `json:"verdict_valid"`
}

type CategoryWinner struct {
	Category string `json:"category"`
	Winner   string `json:"winner"`
	Evidence string `json:"evidence"`
	Margin   string `json:"margin"`
}

type TargetVerdict struct {
	Name        string   `json:"name"`
	Strengths   []string `json:"strengths"`
	Weaknesses  []string `json:"weaknesses"`
	BestFor     []string `json:"best_for"`
	AvoidUnless []string `json:"avoid_unless"`
}

type CodeSnippet struct {
	Target       string `json:"target"`
	FilePath     string `json:"file_path"`
	Label        string `json:"label"`
	StartLine    int    `json:"start_line"`
	EndLine      int    `json:"end_line"`
	Language     string `json:"language"`
	Snippet      string `json:"snippet"`
	WhyItMatters string `json:"why_it_matters"`
}

type TerminalHighlight struct {
	Target       string  `json:"target"`
	Label        string  `json:"label"`
	Command      *string `json:"command"`
	Output       string  `json:"output"`
	Category     string  `json:"category"`
	WhyItMatters string  `json:"why_it_matters"`
}

type EnvironmentInfo struct {
	OS                   string      `json:"os"`
	OSVersion            string      `json:"os_version"`
	CPU                  string      `json:"cpu"`
	RAMGB                float64     `json:"ram_gb"`
	DockerVersion        *string     `json:"docker_version"`
	Toolchains           []Toolchain `json:"toolchains"`
	BenchmarkTool        string      `json:"benchmark_tool"`
	BenchmarkToolVersion string      `json:"benchmark_tool_version"`
	Timestamp            string      `json:"timestamp"`
	Hostname             string      `json:"hostname"`
}

type Toolchain struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	if len(ordered) == 1 {
		return ordered[0]
	}
	rank := (float64(len(ordered)-1) * pct) / 100.0
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if hi >= len(ordered) {
		hi = len(ordered) - 1
	}
	if lo == hi {
		return ordered[lo]
	}
	frac := rank - float64(lo)
	return ordered[lo]*(1-frac) + ordered[hi]*frac
}

func summarize(values []float64) MetricAggregate {
	if len(values) == 0 {
		return MetricAggregate{}
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	sum := 0.0
	for _, v := range ordered {
		sum += v
	}
	mean := sum / float64(len(ordered))
	var variance float64
	for _, v := range ordered {
		diff := v - mean
		variance += diff * diff
	}
	stddev := math.Sqrt(variance / float64(len(ordered)))
	return MetricAggregate{
		Mean:      mean,
		Median:    percentile(ordered, 50),
		P50:       percentile(ordered, 50),
		P95:       percentile(ordered, 95),
		P99:       percentile(ordered, 99),
		Min:       ordered[0],
		Max:       ordered[len(ordered)-1],
		Stddev:    stddev,
		RunValues: append([]float64(nil), ordered...),
	}
}

func parsePercent(s string) (float64, error) {
	s = strings.TrimSpace(strings.TrimSuffix(s, "%"))
	if s == "" {
		return 0, errors.New("empty percent")
	}
	return strconv.ParseFloat(s, 64)
}

func parseBytes(s string) (float64, error) {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	if s == "" {
		return 0, errors.New("empty size")
	}
	multipliers := []struct {
		suffix string
		mult   float64
	}{
		{"GiB", 1024},
		{"MiB", 1},
		{"KiB", 1.0 / 1024},
		{"GB", 1000},
		{"MB", 1},
		{"KB", 1.0 / 1000},
		{"B", 1.0 / (1024 * 1024)},
	}
	for _, m := range multipliers {
		if strings.HasSuffix(s, m.suffix) {
			num := strings.TrimSpace(strings.TrimSuffix(s, m.suffix))
			v, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, err
			}
			return v * m.mult, nil
		}
	}
	return strconv.ParseFloat(s, 64)
}

func jsonString(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
