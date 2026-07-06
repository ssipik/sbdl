# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

SBDL (Spark Batch Data Load) is a PySpark batch job that reads three source
feeds — `accounts`, `parties`, `party_address` — joins them, and emits a
denormalized "SBDL-Contract" CDC-style event per account as JSON (see
`test_data/results/final_df.json` for the target output shape: each record has
an `eventHeader`, `keys`, and a `payload` with `operation`/`newValue` wrappers
per field, plus a nested `partyRelations` array with embedded `partyAddress`).
The job is designed to run against different environments (LOCAL/QA/PROD)
selected by a CLI arg, driven by `.conf` files under `conf/`.

The codebase is an early-stage starter: `lib/Utils.py` and `lib/logger.py`
exist, but the actual load/transform/CDC-event logic has not been built yet —
`sbdl_main.py` currently only creates a Spark session and logs. Treat
`test_data/` (input CSVs + expected `final_df.json`) as the spec for the
transform to build.

## Commands

This project uses `uv` (see `pyproject.toml`, `uv.lock`) for the dev
environment; a `Pipfile` also exists for the Jenkins pipeline (`pipenv`).

```bash
# install deps (uv)
uv sync

# run all tests
uv run pytest

# run a single test
uv run pytest test_pytest_sbdl.py::test_blank_test

# run the job locally (env, load_date)
uv run python sbdl_main.py local 2022-08-02
```

`.vscode/launch.json` has matching debug configs ("SBDL" launches
`sbdl_main.py` with `local`; "Test SBDL" runs pytest).

Note: `pytest` asserts an exact Spark version (`spark.version == "4.0.3"`) —
update that assertion if the pinned `pyspark` version changes.

## Architecture

- `sbdl_main.py` — entry point. Takes CLI args `{local|qa|prod} {load_date}`,
  builds the Spark session via `lib.Utils.get_spark_session`, and initializes
  the `Log4j` logger. This is where the read → join → transform → write
  pipeline should be assembled.
- `lib/Utils.py` — environment-aware Spark session factory. `LOCAL` runs
  `local[2]` with log4j config pointed at `log4j.properties`; `QA`/`PROD` rely
  on cluster defaults (`spark-submit`/YARN) and just call
  `.enableHiveSupport().getOrCreate()`.
- `lib/logger.py` — thin wrapper (`Log4j`) around the JVM log4j logger
  reachable through `spark._jvm`, exposing `info`/`warn`/`error`/`debug`.
- `conf/sbdl.conf`, `conf/spark.conf` — INI-style, one section per environment
  (`[LOCAL]`, `[QA]`, `[PROD]`): Hive enablement/database, Kafka topic, and
  Spark resource settings (executors, cores, memory, shuffle partitions)
  respectively. Any config-reading code should key off the uppercased env arg
  to pick the matching section.
- `test_data/` — sample input CSVs for the three source feeds plus
  `results/final_df.json`, the expected joined/transformed output. Field
  naming shows the source→target mapping, e.g. `account_id` →
  `contractIdentifier`, `legal_title_1`/`legal_title_2` →
  `contractTitle[].contractTitleLine` (`lgl_ttl_ln_1`/`lgl_ttl_ln_2`),
  `tax_id_type`/`tax_id` → `taxIdentifier`.
- `sbdl_submit.sh` / `Jenkinsfile` — deployment path: CI zips `lib/` into
  `sbdl.zip`, then `scp`s `sbdl.zip`, `log4j.properties`, `sbdl_main.py`,
  `sbdl_submit.sh`, and `conf/` to a remote edge node (QA on the `release`
  branch, PROD on `master`). `spark-submit` on the edge node runs with
  `--py-files sbdl.zip --files conf/sbdl.conf,conf/spark.conf,log4j.properties`.
  Keep filenames in `sbdl_submit.sh` in sync with what the Jenkinsfile actually
  packages/ships (currently `sdbl_lib.zip`/`sdbl.conf` there are typos vs.
  `sbdl.zip`/`sbdl.conf` elsewhere — check before relying on this script).
