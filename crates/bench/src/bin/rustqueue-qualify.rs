use anyhow::{bail, Context};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

const SCENARIOS: [&str; 3] = ["raw_write", "sustainable", "low_load_latency"];

#[derive(Parser, Debug)]
#[command(
    name = "rustqueue-qualify",
    about = "Evaluate paired RustQueue Broker qualification runs"
)]
struct Args {
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
struct QualificationInput {
    schema_version: u32,
    release: String,
    generated_at_utc: String,
    baseline: Revision,
    candidate: Revision,
    environment: Value,
    protocol: Protocol,
    runs: Vec<Run>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Revision {
    revision: String,
    commit: String,
    image_id: String,
    binary_sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Protocol {
    pairs: usize,
    warmup_seconds: u64,
    measurement_seconds: u64,
    drain_timeout_seconds: u64,
    alternating_order: String,
    bootstrap_iterations: usize,
    bootstrap_seed: u64,
    throughput_regression_ratio: f64,
    latency_rss_regression_ratio: f64,
    scenarios: Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Variant {
    Baseline,
    Candidate,
}

#[derive(Debug, Deserialize, Serialize)]
struct Run {
    #[serde(rename = "case")]
    scenario: String,
    pair: usize,
    sequence: usize,
    position_in_pair: usize,
    variant: Variant,
    commit: String,
    benchmark_exit_code: i32,
    metrics: Metrics,
}

#[derive(Debug, Deserialize, Serialize)]
struct Metrics {
    messages: u64,
    received_unique_messages: u64,
    duplicate_messages: u64,
    missing_messages: u64,
    delivery_verified: bool,
    delivery_complete: bool,
    drain_timed_out: bool,
    final_channel_depth: Option<u64>,
    final_in_flight: Option<u64>,
    final_deferred: Option<u64>,
    publish_messages_per_second: f64,
    receive_messages_per_second: Option<f64>,
    pub_ack_p99_us: u64,
    rss_peak_bytes: u64,
    broker_profile: BrokerProfile,
}

#[derive(Debug, Deserialize, Serialize)]
struct BrokerProfile {
    publish_group_commits: u64,
    publish_group_requests: u64,
    publish_group_max_requests: u64,
    channel_group_commits: u64,
    channel_group_requests: u64,
    channel_group_max_requests: u64,
    channel_fsync_count: u64,
    channel_fsync_sum_seconds: f64,
    channel_group_wait_count: u64,
    channel_group_wait_sum_seconds: f64,
    consumer_fetch_batches: u64,
    consumer_fetch_messages: u64,
    aggregate_channel_depth: u64,
    aggregate_channel_in_flight: u64,
    aggregate_channel_deferred: u64,
}

#[derive(Debug, Serialize)]
struct QualificationEvidence {
    schema_version: u32,
    release: String,
    generated_at_utc: String,
    baseline: Revision,
    candidate: Revision,
    environment: Value,
    protocol: Protocol,
    runs: Vec<Run>,
    statistics: Vec<Statistic>,
    verdict: Verdict,
}

#[derive(Debug, Serialize)]
struct Statistic {
    #[serde(rename = "case")]
    scenario: &'static str,
    metric: &'static str,
    direction: &'static str,
    estimator: &'static str,
    candidate_over_baseline: f64,
    bootstrap_p05: f64,
    bootstrap_p95: f64,
    one_sided_95_bound: Bound,
    regression_threshold: f64,
    regression: bool,
    statistically_significant_improvement: bool,
    bootstrap_seed: u64,
}

#[derive(Debug, Serialize)]
struct Bound {
    kind: &'static str,
    value: f64,
}

#[derive(Debug, Serialize)]
struct Verdict {
    status: &'static str,
    hard_failures: Vec<String>,
    regressions: Vec<String>,
}

#[derive(Clone, Copy)]
enum Metric {
    PublishThroughput,
    ReceiveThroughput,
    PubAckP99,
    PeakRss,
}

#[derive(Clone, Copy)]
enum Direction {
    HigherIsBetter,
    LowerIsBetter,
}

struct Gate {
    scenario: &'static str,
    metric: Metric,
    direction: Direction,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let document = fs::read(&args.input)
        .with_context(|| format!("read qualification input {}", args.input.display()))?;
    let input: QualificationInput = serde_json::from_slice(&document)
        .with_context(|| format!("parse qualification input {}", args.input.display()))?;
    let evidence = evaluate(input)?;
    let passed = evidence.verdict.status == "pass";
    println!("{}", serde_json::to_string_pretty(&evidence)?);
    if !passed {
        bail!("Broker qualification failed");
    }
    Ok(())
}

fn evaluate(input: QualificationInput) -> anyhow::Result<QualificationEvidence> {
    validate_input(&input)?;
    let enabled_scenarios = scenario_names(&input.protocol)?;
    let gates = [
        Gate {
            scenario: "raw_write",
            metric: Metric::PublishThroughput,
            direction: Direction::HigherIsBetter,
        },
        Gate {
            scenario: "sustainable",
            metric: Metric::ReceiveThroughput,
            direction: Direction::HigherIsBetter,
        },
        Gate {
            scenario: "low_load_latency",
            metric: Metric::PubAckP99,
            direction: Direction::LowerIsBetter,
        },
        Gate {
            scenario: "low_load_latency",
            metric: Metric::PeakRss,
            direction: Direction::LowerIsBetter,
        },
    ];
    let mut statistics = Vec::with_capacity(gates.len());
    for gate in gates
        .into_iter()
        .filter(|gate| enabled_scenarios.contains(&gate.scenario))
    {
        statistics.push(evaluate_gate(&input, &gate)?);
    }
    let regressions = statistics
        .iter()
        .filter(|statistic| statistic.regression)
        .map(|statistic| format!("{}:{}", statistic.scenario, statistic.metric))
        .collect::<Vec<_>>();
    let status = if regressions.is_empty() {
        "pass"
    } else {
        "fail"
    };

    Ok(QualificationEvidence {
        schema_version: input.schema_version,
        release: input.release,
        generated_at_utc: input.generated_at_utc,
        baseline: input.baseline,
        candidate: input.candidate,
        environment: input.environment,
        protocol: input.protocol,
        runs: input.runs,
        statistics,
        verdict: Verdict {
            status,
            hard_failures: Vec::new(),
            regressions,
        },
    })
}

fn validate_input(input: &QualificationInput) -> anyhow::Result<()> {
    if input.schema_version != 1 {
        bail!(
            "unsupported qualification schema version {}",
            input.schema_version
        );
    }
    if input.protocol.pairs == 0 {
        bail!("qualification requires at least one pair");
    }
    if input.protocol.warmup_seconds == 0
        || input.protocol.measurement_seconds == 0
        || input.protocol.drain_timeout_seconds == 0
    {
        bail!("qualification timing values must be greater than zero");
    }
    if input.protocol.bootstrap_iterations == 0 {
        bail!("bootstrap-iterations must be greater than zero");
    }
    if input.protocol.alternating_order != "AB_then_BA" {
        bail!("alternating-order must be AB_then_BA");
    }
    if !(0.0..1.0).contains(&input.protocol.throughput_regression_ratio) {
        bail!("throughput-regression-ratio must be between zero and one");
    }
    if input.protocol.latency_rss_regression_ratio <= 1.0 {
        bail!("latency-rss-regression-ratio must be greater than one");
    }

    let scenarios = scenario_names(&input.protocol)?;
    let expected_runs = scenarios.len() * input.protocol.pairs * 2;
    if input.runs.len() != expected_runs {
        bail!(
            "qualification has {} runs, expected {expected_runs}",
            input.runs.len()
        );
    }
    for scenario in scenarios {
        for pair in 1..=input.protocol.pairs {
            let mut paired = input
                .runs
                .iter()
                .filter(|run| run.scenario == scenario && run.pair == pair)
                .collect::<Vec<_>>();
            paired.sort_by_key(|run| run.position_in_pair);
            if paired.len() != 2 {
                bail!("{scenario} pair {pair} must contain exactly two runs");
            }
            let expected = if pair % 2 == 1 {
                [Variant::Baseline, Variant::Candidate]
            } else {
                [Variant::Candidate, Variant::Baseline]
            };
            for (index, run) in paired.into_iter().enumerate() {
                if run.position_in_pair != index + 1 || run.variant != expected[index] {
                    bail!("{scenario} pair {pair} violates the AB_then_BA order contract");
                }
                validate_run(input, run)?;
            }
        }
    }
    Ok(())
}

fn scenario_names(protocol: &Protocol) -> anyhow::Result<Vec<&str>> {
    let scenarios = protocol
        .scenarios
        .as_array()
        .context("protocol scenarios must be an array")?;
    if scenarios.is_empty() {
        bail!("protocol must enable at least one qualification case");
    }
    let mut names = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let name = scenario
            .get("name")
            .and_then(Value::as_str)
            .context("each protocol scenario must have a string name")?;
        if !SCENARIOS.contains(&name) {
            bail!("unknown protocol case {name}");
        }
        if names.contains(&name) {
            bail!("protocol case {name} is duplicated");
        }
        names.push(name);
    }
    Ok(names)
}

fn validate_run(input: &QualificationInput, run: &Run) -> anyhow::Result<()> {
    if !SCENARIOS.contains(&run.scenario.as_str()) {
        bail!("unknown qualification case {}", run.scenario);
    }
    let expected_commit = match run.variant {
        Variant::Baseline => &input.baseline.commit,
        Variant::Candidate => &input.candidate.commit,
    };
    if &run.commit != expected_commit {
        bail!(
            "{} pair {} has commit {}, expected {}",
            run.scenario,
            run.pair,
            run.commit,
            expected_commit
        );
    }
    if run.benchmark_exit_code != 0 {
        bail!(
            "{} pair {} {:?} benchmark exited with {}",
            run.scenario,
            run.pair,
            run.variant,
            run.benchmark_exit_code
        );
    }
    let metrics = &run.metrics;
    if metrics.messages == 0
        || !positive_finite(metrics.publish_messages_per_second)
        || metrics.pub_ack_p99_us == 0
        || metrics.rss_peak_bytes == 0
    {
        bail!(
            "{} pair {} {:?} contains an invalid core metric",
            run.scenario,
            run.pair,
            run.variant
        );
    }
    if run.scenario == "raw_write" {
        if metrics.delivery_verified {
            bail!("raw_write must not start consumers");
        }
    } else {
        if !metrics.delivery_verified
            || !metrics.delivery_complete
            || metrics.drain_timed_out
            || metrics.received_unique_messages != metrics.messages
            || metrics.missing_messages != 0
            || metrics.duplicate_messages != 0
            || metrics.final_channel_depth != Some(0)
            || metrics.final_in_flight != Some(0)
            || metrics.final_deferred != Some(0)
            || metrics.broker_profile.aggregate_channel_depth != 0
            || metrics.broker_profile.aggregate_channel_in_flight != 0
            || metrics.broker_profile.aggregate_channel_deferred != 0
        {
            bail!(
                "{} pair {} {:?} failed the complete-delivery contract",
                run.scenario,
                run.pair,
                run.variant
            );
        }
        if !metrics
            .receive_messages_per_second
            .is_some_and(positive_finite)
        {
            bail!(
                "{} pair {} {:?} has invalid receive throughput",
                run.scenario,
                run.pair,
                run.variant
            );
        }
    }
    Ok(())
}

fn positive_finite(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

fn evaluate_gate(input: &QualificationInput, gate: &Gate) -> anyhow::Result<Statistic> {
    let mut pairs = BTreeMap::<usize, (Option<f64>, Option<f64>)>::new();
    for run in input
        .runs
        .iter()
        .filter(|run| run.scenario == gate.scenario)
    {
        let value = metric_value(&run.metrics, gate.metric)?;
        let values = pairs.entry(run.pair).or_default();
        match run.variant {
            Variant::Baseline => values.0 = Some(value),
            Variant::Candidate => values.1 = Some(value),
        }
    }
    let ratios = pairs
        .into_iter()
        .map(|(pair, (baseline, candidate))| {
            let baseline = baseline.context(format!("pair {pair} is missing baseline"))?;
            let candidate = candidate.context(format!("pair {pair} is missing candidate"))?;
            if !positive_finite(baseline) || !positive_finite(candidate) {
                bail!("pair {pair} contains a non-positive metric");
            }
            Ok(candidate / baseline)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let seed = input.protocol.bootstrap_seed ^ stable_hash(gate.scenario, metric_name(gate.metric));
    let mut distribution =
        bootstrap_geometric_means(&ratios, input.protocol.bootstrap_iterations, seed);
    distribution.sort_by(f64::total_cmp);
    let point = geometric_mean(&ratios);
    let p05 = quantile(&distribution, 0.05);
    let p95 = quantile(&distribution, 0.95);
    let (bound, threshold, regression, improvement) = match gate.direction {
        Direction::HigherIsBetter => (
            Bound {
                kind: "upper",
                value: p95,
            },
            input.protocol.throughput_regression_ratio,
            p95 < input.protocol.throughput_regression_ratio,
            p05 > 1.0,
        ),
        Direction::LowerIsBetter => (
            Bound {
                kind: "lower",
                value: p05,
            },
            input.protocol.latency_rss_regression_ratio,
            p05 > input.protocol.latency_rss_regression_ratio,
            p95 < 1.0,
        ),
    };
    Ok(Statistic {
        scenario: gate.scenario,
        metric: metric_name(gate.metric),
        direction: match gate.direction {
            Direction::HigherIsBetter => "higher_is_better",
            Direction::LowerIsBetter => "lower_is_better",
        },
        estimator: "geometric_mean_of_paired_ratios",
        candidate_over_baseline: point,
        bootstrap_p05: p05,
        bootstrap_p95: p95,
        one_sided_95_bound: bound,
        regression_threshold: threshold,
        regression,
        statistically_significant_improvement: improvement,
        bootstrap_seed: seed,
    })
}

fn metric_value(metrics: &Metrics, metric: Metric) -> anyhow::Result<f64> {
    let value = match metric {
        Metric::PublishThroughput => metrics.publish_messages_per_second,
        Metric::ReceiveThroughput => metrics
            .receive_messages_per_second
            .context("receive throughput is missing")?,
        Metric::PubAckP99 => metrics.pub_ack_p99_us as f64,
        Metric::PeakRss => metrics.rss_peak_bytes as f64,
    };
    Ok(value)
}

fn metric_name(metric: Metric) -> &'static str {
    match metric {
        Metric::PublishThroughput => "publish_messages_per_second",
        Metric::ReceiveThroughput => "receive_messages_per_second",
        Metric::PubAckP99 => "pub_ack_p99_us",
        Metric::PeakRss => "rss_peak_bytes",
    }
}

fn geometric_mean(values: &[f64]) -> f64 {
    (values.iter().map(|value| value.ln()).sum::<f64>() / values.len() as f64).exp()
}

fn bootstrap_geometric_means(values: &[f64], iterations: usize, seed: u64) -> Vec<f64> {
    let mut rng = SplitMix64::new(seed);
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let mut log_sum = 0.0;
        for _ in 0..values.len() {
            log_sum += values[rng.index(values.len())].ln();
        }
        samples.push((log_sum / values.len() as f64).exp());
    }
    samples
}

fn quantile(sorted: &[f64], probability: f64) -> f64 {
    let position = probability * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let fraction = position - lower as f64;
        sorted[lower] * (1.0 - fraction) + sorted[upper] * fraction
    }
}

fn stable_hash(scenario: &str, metric: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in scenario.bytes().chain([b':']).chain(metric.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn index(&mut self, upper: usize) -> usize {
        ((u128::from(self.next()) * upper as u128) >> 64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paired_bootstrap_is_deterministic() {
        let values = [0.97, 1.01, 1.03, 0.99];
        assert_eq!(
            bootstrap_geometric_means(&values, 100, 802),
            bootstrap_geometric_means(&values, 100, 802)
        );
    }

    #[test]
    fn clear_regressions_cross_the_one_sided_bounds() {
        let mut throughput = bootstrap_geometric_means(&[0.90; 10], 1_000, 802);
        throughput.sort_by(f64::total_cmp);
        assert!(quantile(&throughput, 0.95) < 0.95);

        let mut latency = bootstrap_geometric_means(&[1.20; 10], 1_000, 802);
        latency.sort_by(f64::total_cmp);
        assert!(quantile(&latency, 0.05) > 1.10);
    }

    #[test]
    fn noise_straddling_a_threshold_does_not_fail_the_gate() {
        let mut values = bootstrap_geometric_means(
            &[0.80, 0.90, 0.94, 0.97, 1.0, 1.01, 1.02, 1.03, 1.04, 1.05],
            10_000,
            802,
        );
        values.sort_by(f64::total_cmp);
        assert!(quantile(&values, 0.95) >= 0.95);
    }
}
