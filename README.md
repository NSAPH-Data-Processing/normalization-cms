# CMS Normalization

A modular, DuckDB-based pipeline for normalizing CMS data. Designed to work with any CMS dataset — Medicaid, Medicare,Medicare, or others. All dataset-specific logic lives in configuration files; no code changes are needed to add or switch datasets. Medicaid TAF (2014–2018) is used as the reference example throughout this documentation.

------------------------------------------------------------------------

## Repository Structure

    ├── conf/
    │   ├── config.yml                  ← Hydra config (entry point for all runner scripts)
    │   ├── snakemake.yaml              ← Snakemake config (dataset selection)
    │   ├── datapaths/
    │   │   └── medicaid_taf_red.yaml   ← input/intermediate/output paths
    │   ├── table_config/
    │   │   └── medicaid_taf/           ← one .yml per table, namespaced by dataset
    │   │       ├── beneficiaries.yml
    │   │       ├── eligibility.yml
    │   │       ├── enrollments.yml
    │   │       ├── ip_header.yml
    │   │       ├── ip_line.yml
    │   │       └── ip_occ.yml
    │   └── build_tables/
    │       └── medicaid_taf.yml        ← Stage 2 aggregation config
    ├── normalizecms/
    │   ├── __init__.py
    │   ├── utils.py                    ← shared utilities (glob helper)
    │   ├── select_variables.py         ← Stage 1 library
    │   ├── build_tables.py             ← Stage 2 library
    │   ├── qc.py                       ← QC library
    │   └── create_dir_paths.py         ← directory initialization utility
    ├── Snakefile                       ← Snakemake workflow (orchestrates all stages)
    ├── run_select_variables.py         ← Stage 1 Hydra entry point
    ├── run_build_tables.py             ← Stage 2 Hydra entry point
    ├── run_qc.py                       ← QC Hydra entry point
    ├── select_variables.sbatch         ← SLURM array job for Stage 1 (manual)
    └── build_tables.sbatch             ← SLURM job for Stage 2 (manual)

------------------------------------------------------------------------

## Workflow

The pipeline runs in two stages plus a QC step.

### Stage 1 — Year-Specific Tables (`select_variables.py`)

Processes one table for a single year. Designed to be parallelized across years.

- Globs raw CMS parquet files for the given year (path resolved by Hydra at runtime)
- Selects columns listed in the table config (types inferred from the parquet schema)
- Optionally explodes 12-element monthly array columns into long format (one row per period)
- Writes output to `{intermediate}/{output_name}.parquet`

Example output per year (Medicaid TAF):

    beneficiaries_2018.parquet
    eligibility_2018.parquet
    enrollment_2018.parquet
    ip_header_2018.parquet
    ip_line_2018.parquet
    ip_occ_2018.parquet

### Stage 2 — Multi-Year Tables (`build_tables.py`)

Concatenates per-year files from Stage 1 across all years. Optionally aggregates by primary key.

- Verifies all per-year input files exist before doing any work
- Loads all yearly parquet files in a single DuckDB parallel scan
- If `primary_key` is configured: aggregates rows, collecting non-key columns into arrays and computing `n_distinct_*` conflict indicators — writes `_array.parquet`
- Always writes a flat `.parquet` (concatenated, unaggregated)

Example output (Medicaid TAF):

    beneficiaries_2014_2018.parquet
    beneficiaries_2014_2018_array.parquet   (primary_key configured)
    eligibility_2014_2018.parquet

### QC (`qc.py`)

Runs quality checks on a normalized parquet file for a single table and year.

- Verifies output file exists and row count is non-zero
- Captures schema before (input) and after (output)
- Computes null counts per column
- Duplicate check: key uniqueness if `primary_key` configured, full-row uniqueness otherwise
- Saves a 100-row sample parquet

Output written to `{intermediate}/{table}_{year}.qc/`:

    qc.json
    schema_before.json
    schema_after.json
    sample.parquet

------------------------------------------------------------------------

## Running the Pipeline

### With Snakemake (recommended)

Snakemake orchestrates all three stages automatically, resolving dependencies and
running jobs in parallel. It reads `conf/snakemake.yaml` for dataset selection and
`conf/build_tables/{dataset}.yml` for the full set of tables and year ranges.

```bash
# Dry run — see exactly what would execute without running anything
snakemake --configfile conf/snakemake.yaml --cores all --dry-run

# Local execution
snakemake --configfile conf/snakemake.yaml --cores all

# SLURM (requires snakemake-executor-plugin-slurm)
snakemake --configfile conf/snakemake.yaml --executor slurm --jobs 50
```

Run a subset of the pipeline:

```bash
# Stop after Stage 1 (skip Stage 2 and QC)
snakemake --configfile conf/snakemake.yaml --cores all --until select_variables

# Skip QC
snakemake --configfile conf/snakemake.yaml --cores all --omit-from qc

# Run a specific table-year only
snakemake --configfile conf/snakemake.yaml /path/to/intermediate/beneficiaries_2018.parquet
```

Switch datasets:

```bash
snakemake --configfile conf/snakemake.yaml \
    --config dataset=medicaid_max datapaths=medicaid_max \
    --cores all
```

### Manually (single table/year)

Runner scripts can be called directly. Hydra loads `conf/config.yml` and any
key can be overridden at the CLI.

```bash
# Stage 1
python run_select_variables.py table_config=medicaid_taf/ip_header year=2018

# Stage 2
python run_build_tables.py dataset=medicaid_taf table=beneficiaries

# QC
python run_qc.py table_config=medicaid_taf/ip_header year=2018
```
------------------------------------------------------------------------

## Snakemake Orchestration

### How it works

Snakemake works **backwards from targets**. The `rule all` block declares the desired
end state — Stage 2 parquets and QC artifacts for every table — and Snakemake traces
backwards through rules to determine what needs to run.

Dependency graph per table:

    select_variables (table, 2014) ─┐
    select_variables (table, 2015) ─┤──► build_tables (table)   ← Stage 2
    select_variables (table, 2016) ─┤
    select_variables (table, 2017) ─┤         ↑ independent
    select_variables (table, 2018) ─┘
            │
            ├──► qc (table, 2014)
            ├──► qc (table, 2015)             ← QC
            ...

All Stage 1 jobs across all tables and years are fully independent and run in parallel.
Once a table's Stage 1 jobs finish, its Stage 2 job and QC jobs start immediately
without waiting for other tables. Stage 2 and QC are independent of each other and
also run concurrently.

### What drives which jobs run

The Snakefile reads `conf/build_tables/{dataset}.yml` to enumerate every table and
its year range. Adding a table or bumping a `year_max` in that file automatically
extends what Snakemake will run — no Snakefile changes needed.

Snakemake checks whether output files already exist. If `beneficiaries_2018.parquet`
is already on disk, Snakemake skips that job. This makes incremental processing
efficient: adding a new year reruns only the missing outputs.

### Resource configuration

Each rule declares its SLURM resource requirements:

| Rule | Memory | CPUs | Time |
|---|---|---|---|
| `select_variables` | 300 GB | 16 | 4 h |
| `build_tables` | 480 GB | 16 | 8 h |
| `qc` | 32 GB | 4 | 1 h |

These can be overridden in the Snakefile's `resources:` block per rule.

------------------------------------------------------------------------

## Configuration System

The pipeline uses two separate configuration systems — **Hydra** (for runner scripts)
and **Snakemake** (for workflow orchestration) — that share some of the same YAML
files but use them differently.

### Config files and their consumers

```
conf/snakemake.yaml
  └── Read by: Snakemake (configfile directive)
      Purpose: selects dataset and datapaths for a Snakemake run
      Keys: dataset, datapaths

conf/config.yml
  └── Read by: Hydra (via @hydra.main in all runner scripts)
      Purpose: Hydra config skeleton — declares which config groups to compose
               and provides default values overridden by Snakemake at runtime
      Keys: defaults list, dataset, table, year, hydra settings

conf/datapaths/<dataset>.yaml
  └── Read by:
      • Snakemake — opens directly to get INTERMEDIATE and OUTPUT paths for
        defining file targets
      • Hydra — loads via the defaults list; values flow into table configs
        via OmegaConf interpolation (${datapaths.dirs.input}, etc.)

conf/table_config/<dataset>/<table>.yml
  └── Read by: Hydra — loaded as a config group for Stage 1 and QC runners
      Purpose: per-table column selection, path patterns, output naming
      Note: paths use OmegaConf interpolation (${datapaths.dirs.input}),
            resolved to real strings before Python library code runs

conf/build_tables/<dataset>.yml
  └── Read by:
      • Snakemake — enumerates tables and year ranges to generate all job targets
      • run_build_tables.py — loaded directly via yaml.safe_load at runtime
      Purpose: Stage 2 aggregation rules (year ranges, output names, primary keys)
```

### How a Snakemake run uses these configs

1. Snakemake reads `conf/snakemake.yaml` → gets `dataset` and `datapaths`
2. Snakemake opens `conf/datapaths/{datapaths}.yaml` → gets `INTERMEDIATE` and `OUTPUT` paths for defining file targets
3. Snakemake opens `conf/build_tables/{dataset}.yml` → gets all tables and year ranges, generates the complete job graph
4. For each job, Snakemake calls a runner script with explicit Hydra CLI overrides
   (e.g. `table_config=medicaid_taf/ip_header year=2018 datapaths=medicaid_taf_red`)
5. The runner script starts, Hydra loads `conf/config.yml`, applies the CLI overrides,
   composes `conf/datapaths/medicaid_taf_red.yaml` and `conf/table_config/medicaid_taf/ip_header.yml`
6. OmegaConf resolves all `${...}` interpolations to real paths
7. The runner passes a plain resolved dict to the library function — no config system awareness needed in the library

### Key design principle

`conf/datapaths/` and `conf/build_tables/` are shared between Snakemake and Hydra.
This means there is **one source of truth** for paths and table definitions — updating
a path or year range in one place updates both systems automatically.

`conf/snakemake.yaml` and `conf/config.yml` are system-specific and are never read
by the other system.

------------------------------------------------------------------------

## Configuration Reference

### `conf/datapaths/<dataset>.yaml`

```yaml
name: medicaid_taf_red
dirs:
  input:        "/path/to/harmonized_medicaid_taf"
  intermediate: "/path/to/normalized_medicaid_taf/intermediate"
  output:       "/path/to/normalized_medicaid_taf/normalized"
```

### `conf/table_config/<dataset>/<table>.yml`

```yaml
name: ip_header
path_pattern: "${datapaths.dirs.input}/${year}/taf_inpatient_header_*/part-*.parquet"
output_name: "ip_header_${year}"
output_dir: "${datapaths.dirs.intermediate}"
columns:
  - bene_id
  - clm_id
  - srvc_bgn_dt
```

Optional keys:
- `explode_arrays` — explode 12-element monthly array columns into one row per period (default: false)
- `period_col` — name of the generated period index column (default: `month`)
- `n_periods` — number of periods to explode (default: 12)

### `conf/build_tables/<dataset>.yml`

```yaml
tables:
  beneficiaries:
    year_min: 2014
    year_max: 2018
    path_pattern: "{basepath}/beneficiaries_{year}.parquet"
    output_name: "beneficiaries_{year_min}_{year_max}"
    primary_key:
      - bene_id
```

Optional per-table keys:
- `primary_key` — triggers array aggregation and conflict indicators
- `threads` — DuckDB thread count (default: 8)
- `memory_limit` — DuckDB memory limit (default: 64GB)

------------------------------------------------------------------------

## Adding a Dataset

No Python changes are needed. Add the following config files:

**1.** `conf/datapaths/<dataset>.yaml`
```yaml
name: medicaid_max
dirs:
  input:        "/path/to/raw_medicaid_max"
  intermediate: "/path/to/normalized_medicaid_max/intermediate"
  output:       "/path/to/normalized_medicaid_max/normalized"
```

**2.** `conf/table_config/<dataset>/<table>.yml` (one per table)
```yaml
name: enrollment
path_pattern: "${datapaths.dirs.input}/${year}/max_ps_*/part-*.parquet"
output_name: "enrollment_${year}"
output_dir: "${datapaths.dirs.intermediate}"
columns:
  - bene_id
  - ...
```

**3.** `conf/build_tables/<dataset>.yml`
```yaml
tables:
  enrollment:
    year_min: 2010
    year_max: 2018
    path_pattern: "{basepath}/enrollment_{year}.parquet"
    output_name: "enrollment_{year_min}_{year_max}"
```

**4.** Run with dataset overrides:
```bash
snakemake --configfile conf/snakemake.yaml \
    --config dataset=medicaid_max datapaths=medicaid_max \
    --cores all
```

------------------------------------------------------------------------

## Conflict Indicators

For tables with `primary_key` configured, Stage 2 produces a `_array.parquet` file.
Each non-key column is aggregated into an array across all rows sharing the same key.
A paired `n_distinct_{col}` column counts the number of distinct non-null values in
that array. A value greater than 1 means the same entity has conflicting values for
that field — these conflicts are resolved in the downstream cleaning step.
