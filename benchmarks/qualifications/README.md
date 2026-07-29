# Broker qualification evidence

`scripts/benchmark-qualify.sh` is the pre-release RustQueue Broker performance
gate. Run it on OrbStack from a committed candidate:

```sh
make benchmark-qualify
```

The default protocol compares the exact `v0.8.1` tag with `HEAD`, uses fresh
Docker volumes, fixes Broker and load-generator containers at 2 vCPU / 2 GiB,
and runs all three release cases as 10 alternating pairs. A full run writes the
reviewable release artifact to `v0.8.2-orbstack.json`.

Per-run benchmark JSON, stderr, RSS samples and the evaluator input stay under
the ignored `benchmarks/results/` directory. Development runs may shorten the
timings or select cases with environment variables, but the script refuses to
publish those results into this directory.
