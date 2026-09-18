# CMS Normalization

A modular, DuckDB-based pipeline for normalizing CMS data. Designed to work with any CMS dataset — Medicaid TAF, Medicare, MAX files, or others. All dataset-specific logic lives in configuration files; no code changes are needed to add or switch datasets. Medicaid MAX-TAF (2000–2018) is used as the reference example throughout this documentation.

------------------------------------------------------------------------

## Repository Structure

    ├── conf/
    │   ├── config.yaml                 ← Hydra config (entry point for all runner scripts)
    │   ├── snakemake.yaml              ← Snakemake config (dataset selection)
    │   ├── datapaths/
    │   │   └── medicaid_taf_red.yaml   ← input/intermediate/output paths
    │   ├── table_config/
    │   │   └── medicaid_max-taf/       ← one .yaml per table, namespaced by dataset
    │   │       ├── admissions.yaml
    │   │       ├── beneficiaries.yaml
    │   │       ├── eligibility.yaml
    │   │       └── enrollments.yaml
    │   └── build_tables/
    │       └── medicaid_max-taf.yaml   ← Stage 2 aggregation config
    ├── normalizecms/
    │   ├── __init__.py
    │   ├── utils.py                    ← shared utilities (glob helper)
    │   ├── select_variables.py         ← Stage 1 library
    │   ├── build_tables.py             ← Stage 2 library
    │   ├── qc.py                       ← Stage 1 run-quality QC library
    │   ├── data_qc.py                  ← Stage 2 data-quality QC library
    │   └── create_dir_paths.py         ← directory initialization utility
    ├── Snakefile                       ← Snakemake workflow (orchestrates all stages)
    ├── run_select_variables.py         ← Stage 1 Hydra entry point
    ├── run_build_tables.py             ← Stage 2 Hydra entry point
    ├── run_qc.py                       ← Stage 1 QC Hydra entry point
    └── run_data_qc.py                  ← Stage 2 data QC Hydra entry point (per-table + aggregate)

------------------------------------------------------------------------

## Workflow

The pipeline runs in two stages plus a run-quality QC step, all orchestrated by Snakemake
(`select_variables` → `build_sharded_table`/`build_flat_table` → `qc`). Stage 2 Data QC
(`data_qc.py`/`run_data_qc.py`) is **not** wired into the Snakefile — it's a separate,
manually-run step (see its section below) rather than something `snakemake --cores all`
triggers automatically.

### Stage 1 — Year-Specific Tables (`select_variables.py`)

Processes one table for a single year. Designed to be parallelized across years.

- Globs raw CMS parquet files for the given year (path resolved by Hydra at runtime)
- Selects columns listed in the table config (types inferred from the parquet schema)
- Optionally explodes 12-element monthly array columns into long format (one row per period,
  `explode_arrays: true`), or unpivots wide `*_mo_NN` monthly columns into long format
  (`monthly_columns: true` — e.g. `eligibility`, which has one column per month per variable
  in the raw source and gets unpivoted into a `month` column instead)
- Writes output to `{intermediate}/{output_name}.parquet`

Example output per year (Medicaid TAF):

    admissions_2018.parquet
    beneficiaries_2018.parquet
    eligibility_2018.parquet
    enrollments_2018.parquet

### Stage 2 — Aggregated Tables (`build_tables.py`)

Loads per-year files from Stage 1 for a table's full `year_min`..`year_max` range in a single
DuckDB scan — this doubles as an implicit schema-drift check across years, since a column type
change or rename in any single year's Stage 1 output surfaces immediately as a read error.
Two independent config keys in `conf/build_tables/<dataset>.yaml` control what happens next:

- **`primary_key`** (optional) — if set, rows are grouped by these columns, non-key columns
  are collected into arrays with paired `n_distinct_*` conflict indicators, and the output
  filename gets an `_array` suffix. If absent, rows are written through unchanged (no suffix).
- **`shard_by`** (optional) — if set, output is split into one file per distinct value of this
  column (must also be a member of `primary_key`, so grouping never merges rows across
  different shard values) instead of one file for the whole range. Written internally via
  DuckDB's native `PARTITION_BY` in a single pass, then flattened to plain files.

**No `shard_by`** — one file covering the full range (e.g. `beneficiaries`):

    beneficiaries_2000_2018_array.parquet

**`shard_by: year`** — one file per year (e.g. `admissions`, `eligibility`, `enrollments`):

    admissions_2000_array.parquet
    admissions_2001_array.parquet
    ...
    admissions_2018_array.parquet

The `_array` suffix always tracks `primary_key`, independent of whether the table is sharded.

### QC — Run Quality (`qc.py`)

Runs quality checks on a normalized parquet file for a single table and year.
These are **run-quality** checks — they verify the normalization step itself worked correctly,
not that the underlying data is clean.

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
running jobs in parallel. `conf/snakemake.yaml` is loaded automatically (via the
Snakefile's own `configfile:` directive) for dataset selection, and
`conf/build_tables/{dataset}.yaml` is read for the full set of tables and year ranges.

```bash
# Dry run — see exactly what would execute without running anything
snakemake --cores all --dry-run

# Local execution
snakemake --cores all

# SLURM (requires snakemake-executor-plugin-slurm)
snakemake --executor slurm --jobs 50
```

Run a subset of the pipeline:

```bash
# Stop after Stage 1 (skip Stage 2 and QC)
snakemake --cores all --until select_variables

# Skip QC
snakemake --cores all --omit-from qc

# Run a specific table-year only
snakemake --cores all /path/to/intermediate/beneficiaries_2018.parquet
```

Switch datasets:

```bash
snakemake --cores all --config dataset=medicaid_max datapaths=medicaid_max
```

### Manually (single table/year)

Runner scripts can be called directly. Hydra loads `conf/config.yaml` and any
key can be overridden at the CLI.

```bash
# Stage 1
python run_select_variables.py table_config=medicaid_max-taf/admissions year=2018

# Stage 2
python run_build_tables.py dataset=medicaid_max-taf table=beneficiaries

# QC
python run_qc.py table_config=medicaid_max-taf/admissions year=2018
```

------------------------------------------------------------------------

## Snakemake Orchestration

### How it works

Snakemake works **backwards from targets**. The `rule all` block declares the desired
end state — Stage 2 parquets and QC artifacts for every table — and Snakemake traces
backwards through rules to determine what needs to run.

Dependency graph per table:

    select_variables (table, 2000) ─┐
    select_variables (table, 2001) ─┤──► build_sharded_table (table)   ← Stage 2, sharded tables
    ...                              ┤       or build_flat_table (table) ← Stage 2, non-sharded
    select_variables (table, 2018) ─┘         tables
            │
            ├──► qc (table, 2000)
            ├──► qc (table, 2001)             ← QC
            ...

All Stage 1 jobs across all tables and years are fully independent and run in parallel.
Once a table's Stage 1 jobs finish, its Stage 2 job and QC jobs start immediately
without waiting for other tables. Stage 2 and QC are independent of each other and
also run concurrently.

`build_sharded_table` and `build_flat_table` are each a single, genuinely wildcarded rule
(matched against every table in their category via `{table}`) — not one rule generated per
table. That's deliberate: an earlier per-table dynamically-named-rule-generation pattern
reliably triggered a Snakemake 8.4.8 job-dispatch bug where one table's job could silently
execute a *different* table's shell command. See the `feedback_build_rule_table_mixup` project
memory for the full diagnosis if you're touching this part of the Snakefile.

### What drives which jobs run

The Snakefile reads `conf/build_tables/{dataset}.yaml` to enumerate every table and
its year range. Adding a table or bumping a `year_max` in that file automatically
extends what Snakemake will run — no Snakefile changes needed, **with one caveat**:
because `build_sharded_table`/`build_flat_table` are single wildcarded rules rather than
one-per-table, every sharded table must share the same `year_min`/`year_max` and
aggregation status (`primary_key` presence) as every other sharded table, and likewise for
non-sharded tables. The Snakefile asserts this explicitly at parse time and raises a clear
`ValueError` if a new table breaks the assumption, rather than silently producing wrong
output declarations.

Snakemake checks whether output files already exist. If `beneficiaries_2000_2018_array.parquet`
is already on disk, Snakemake skips that job. This makes incremental processing
efficient: adding a new year reruns only the missing outputs.

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

conf/config.yaml
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

conf/table_config/<dataset>/<table>.yaml
  └── Read by: Hydra — loaded as a config group for Stage 1 and QC runners
      Purpose: per-table column selection, path patterns, output naming
      Note: paths use OmegaConf interpolation (${datapaths.dirs.input}),
            resolved to real strings before Python library code runs

conf/build_tables/<dataset>.yaml
  └── Read by:
      • Snakemake — enumerates tables and year ranges to generate all job targets
      • run_build_tables.py — loaded directly via yaml.safe_load at runtime
      Purpose: Stage 2 aggregation rules (year ranges, output names, primary keys)
```

### How a Snakemake run uses these configs

1. Snakemake reads `conf/snakemake.yaml` → gets `dataset` and `datapaths`
2. Snakemake opens `conf/datapaths/{datapaths}.yaml` → gets `INTERMEDIATE` and `OUTPUT` paths for defining file targets
3. Snakemake opens `conf/build_tables/{dataset}.yaml` → gets all tables and year ranges, generates the complete job graph
4. For each job, Snakemake calls a runner script with explicit Hydra CLI overrides
   (e.g. `table_config=medicaid_max-taf/admissions year=2018 datapaths=medicaid_taf_red`)
5. The runner script starts, Hydra loads `conf/config.yaml`, applies the CLI overrides,
   composes `conf/datapaths/medicaid_taf_red.yaml` and `conf/table_config/medicaid_max-taf/admissions.yaml`
6. OmegaConf resolves all `${...}` interpolations to real paths
7. The runner passes a plain resolved dict to the library function — no config system awareness needed in the library

### Key design principle

`conf/datapaths/` and `conf/build_tables/` are shared between Snakemake and Hydra.
This means there is **one source of truth** for paths and table definitions — updating
a path or year range in one place updates both systems automatically.

`conf/snakemake.yaml` and `conf/config.yaml` are system-specific and are never read
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

### `conf/table_config/<dataset>/<table>.yaml`

```yaml
name: admissions
path_pattern: "${datapaths.dirs.input}/inpatient/ip_${year}.parquet"
output_name: "admissions_${year}"
output_dir: "${datapaths.dirs.intermediate}"
columns:
  - bene_id
  - claim_id
  - admission_date
  - srvc_bgn_dt
  - srvc_end_dt
```

Optional keys:
- `explode_arrays` — explode 12-element monthly array columns into one row per period (default: false)
- `monthly_columns` — unpivot wide `*_mo_NN` monthly columns (default matches `^(.+)_mo_(\d{2})$`) into long format, one row per month (default: false; used by `eligibility`)
- `monthly_pattern` — regex overriding the default `*_mo_NN` match pattern for `monthly_columns` (two capture groups: base name, month number)
- `period_col` — name of the generated period index column for either reshape above (default: `month`)
- `n_periods` — number of periods to explode/unpivot (default: 12)

Only one of `explode_arrays`/`monthly_columns` should be set per table.

### `conf/build_tables/<dataset>.yaml`

```yaml
tables:
  beneficiaries:                                    # non-sharded example — one file, full range
    year_min: 2000
    year_max: 2018
    path_pattern: "{basepath}/beneficiaries_{year}.parquet"
    primary_key:
      - bene_id
    output_name: "beneficiaries_{year_min}_{year_max}"

  eligibility:                                      # sharded example — one file per year
    year_min: 2000
    year_max: 2018
    path_pattern: "{basepath}/eligibility_{year}.parquet"
    shard_by: year
    primary_key:
      - bene_id
      - state
      - month
      - year                                        # shard_by column must be in primary_key
    output_name: "eligibility_{year_min}_{year_max}"
```

Required per-table keys:
- `year_min`, `year_max` — year range to process
- `path_pattern` — path template for per-year Stage 1 output files; `{basepath}` and `{year}` are substituted at runtime
- `output_name` — output filename stem template (only used for non-sharded tables; supports `{year_min}`/`{year_max}` substitution — sharded tables are always named `{table}_{shard_value}[_array].parquet` regardless of `output_name`)

Keys required when `primary_key` is set:
- `primary_key` — columns to group by; triggers array aggregation, `n_distinct_*` conflict indicators, and an `_array` output filename suffix

Optional per-table keys:
- `shard_by` — a column name that, if set, must also be a member of `primary_key`; splits output into one file per distinct value of this column (typically `year`) instead of one file for the whole range
- `threads` — DuckDB thread count (default: 4)
- `memory_limit` — DuckDB memory limit (default: 400GB)

**Note on adding tables**: `build_sharded_table`/`build_flat_table` in the Snakefile are each a
single wildcarded rule shared across every table in their category (see "Snakemake
Orchestration" above), which requires every sharded table to share one `year_min`/`year_max`
and aggregation status, and likewise for non-sharded tables. Adding a table that breaks this
raises a clear `ValueError` at Snakefile-parse time rather than silently misbehaving.

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

**2.** `conf/table_config/<dataset>/<table>.yaml` (one per table)
```yaml
name: enrollment
path_pattern: "${datapaths.dirs.input}/${year}/max_ps_*/part-*.parquet"
output_name: "enrollment_${year}"
output_dir: "${datapaths.dirs.intermediate}"
columns:
  - bene_id
  - ...
```

**3.** `conf/build_tables/<dataset>.yaml` — see "Configuration Reference" above for the full
`primary_key`/`shard_by` schema; this minimal example has neither (flat, non-sharded, one file
covering the full range with no `_array` suffix):
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
snakemake --cores all --config dataset=medicaid_max datapaths=medicaid_max
```

------------------------------------------------------------------------

## Conflict Indicators

For tables with `primary_key` configured, Stage 2 produces a `_array.parquet` file.
Each non-key column is aggregated into an array across all rows sharing the same key.
A paired `n_distinct_{col}` column counts the number of distinct non-null values in
that array. A value greater than 1 means the same entity has conflicting values for
that field — these conflicts are resolved in the downstream cleaning step.
