# Broker qualification evidence

`scripts/benchmark-qualify.sh` is the optional RustQueue Broker performance
qualification. Run it on OrbStack from a committed candidate:

```sh
make benchmark-qualify
```

The default protocol compares the exact `v0.8.1` tag with `HEAD`, uses fresh
Docker volumes, fixes Broker and load-generator containers at 2 vCPU / 2 GiB,
and runs all three cases as 10 alternating pairs. A full run writes the
reviewable artifact to `v0.8.2-orbstack.json`. Consumer cases must drain
completely and have a fixed 1,800-second timeout so the v0.8.1 durable `FIN`
baseline is not rejected merely for exceeding a short operational timeout.

The RustQueue 0.8.2 release does not make this optional 60-run artifact a
release metadata requirement. Short development preflights can detect hard
correctness failures and obvious regressions, but they do not substantiate a
formal performance claim.

Per-run benchmark JSON, stderr, RSS samples and the evaluator input stay under
the ignored `benchmarks/results/` directory. Development runs may shorten the
timings or select cases with environment variables, but the script refuses to
publish those results into this directory.
