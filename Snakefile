#Snakefile
import re
import yaml
from pathlib import Path

configfile: "conf/snakemake.yaml"

# ── Load pipeline config ───────────────────────────────────────────────────────

DATASET   = config["dataset"]
DATAPATHS = config["datapaths"]

with open(f"conf/datapaths/{DATAPATHS}.yaml") as _f:
    _dp = yaml.safe_load(_f)
INTERMEDIATE = _dp["dirs"]["intermediate"]
OUTPUT       = _dp["dirs"]["output"]

# Load build_tables config — single file containing all table configs.
_bt_path = Path(f"conf/build_tables/{DATASET}.yaml")
if not _bt_path.exists():
    raise FileNotFoundError(
        f"build_tables config not found: {_bt_path.resolve()}\n"
        f"Run snakemake from the normalization-cms root directory."
    )
with open(_bt_path) as _f:
    TABLE_CONFS = yaml.safe_load(_f)["tables"]
TABLE_NAMES = list(TABLE_CONFS)

SHARDED_TABLES = [t for t in TABLE_NAMES if "shard_by" in TABLE_CONFS[t]]
FLAT_TABLES    = [t for t in TABLE_NAMES if "shard_by" not in TABLE_CONFS[t]]

# build_sharded_table (below) is one Snakemake rule matched against every sharded table via the
# {table} wildcard, with a single static (non-function) `output:` list shared by all of them —
# Snakemake doesn't allow `output:` to be a function of wildcards, so this only works if every
# sharded table's year range is identical. Asserted explicitly here rather than assumed
# silently, so a future config change that breaks this fails loudly instead of producing wrong
# output declarations for whichever table's range changed.
if SHARDED_TABLES:
    _sharded_ranges = {(TABLE_CONFS[t]["year_min"], TABLE_CONFS[t]["year_max"]) for t in SHARDED_TABLES}
    if len(_sharded_ranges) > 1:
        raise ValueError(
            f"build_sharded_table assumes all sharded tables share one year_min/year_max; found "
            f"different ranges: "
            f"{ {t: (TABLE_CONFS[t]['year_min'], TABLE_CONFS[t]['year_max']) for t in SHARDED_TABLES} }. "
            "See feedback_build_rule_table_mixup memory before reintroducing per-table rule "
            "generation to handle this — that pattern caused a Snakemake job-mixup bug here before."
        )
    SHARD_YEAR_MIN, SHARD_YEAR_MAX = next(iter(_sharded_ranges))

# Same reasoning as above, but for aggregation status rather than year range: a sharded table
# with primary_key set gets aggregated (write_sharded_parquet(..., suffix="_array") in
# normalizecms/build_tables.py's aggregate_table()); one without primary_key does not
# (write_flat_table() calls write_sharded_parquet() with no suffix). A single static `output:`
# template can't apply that suffix conditionally per table, so — as with year range — every
# sharded table is asserted to share the same aggregation status. Today all three (admissions,
# eligibility, enrollments) have primary_key, so this holds trivially, but the underlying
# Python code genuinely supports a non-aggregated sharded table (see run_build_tables.py's
# docstring: "sharded + flat"), so this is asserted rather than assumed.
    _sharded_aggregated = {("primary_key" in TABLE_CONFS[t]) for t in SHARDED_TABLES}
    if len(_sharded_aggregated) > 1:
        raise ValueError(
            f"build_sharded_table assumes all sharded tables are uniformly aggregated (have "
            f"primary_key) or uniformly not — found a mix: "
            f"{ {t: ('primary_key' in TABLE_CONFS[t]) for t in SHARDED_TABLES} }. Generalize "
            "build_sharded_table's output: template deliberately to handle both cases (see "
            "feedback_build_rule_table_mixup memory before reintroducing per-table rule "
            "generation)."
        )
    SHARD_SUFFIX = "_array" if "primary_key" in TABLE_CONFS[SHARDED_TABLES[0]] else ""
else:
    SHARD_YEAR_MIN = 0
    SHARD_YEAR_MAX = -1  # range(0, 0) produces empty sequence
    SHARD_SUFFIX = ""
    
# build_flat_table (below) is the non-sharded analog of build_sharded_table above: one rule
# matched against every non-sharded table via {table}, with a single static `output:` template
# shared by all of them. This needs THREE things to hold uniformly across FLAT_TABLES, not just
# one — asserted explicitly, same reasoning as above:
#   1. a shared year_min/year_max,
#   2. a shared aggregation status (all have primary_key, or none do — determines whether the
#      output filename gets an _array suffix),
#   3. each table's real output_name must actually resolve to the plain {table}_{year_min}_
#      {year_max}[_array] convention — a customized output_name template would silently break
#      the static output: string below without this check.
if FLAT_TABLES:
    _flat_ranges = {(TABLE_CONFS[t]["year_min"], TABLE_CONFS[t]["year_max"]) for t in FLAT_TABLES}
    if len(_flat_ranges) > 1:
        raise ValueError(
            f"build_flat_table assumes all non-sharded tables share one year_min/year_max; "
            f"found different ranges: "
            f"{ {t: (TABLE_CONFS[t]['year_min'], TABLE_CONFS[t]['year_max']) for t in FLAT_TABLES} }. "
            "See feedback_build_rule_table_mixup memory before reintroducing per-table rule "
            "generation to handle this."
        )
    FLAT_YEAR_MIN, FLAT_YEAR_MAX = next(iter(_flat_ranges))

    _flat_aggregated = {("primary_key" in TABLE_CONFS[t]) for t in FLAT_TABLES}
    if len(_flat_aggregated) > 1:
        raise ValueError(
            f"build_flat_table assumes all non-sharded tables are uniformly aggregated (have "
            f"primary_key) or uniformly not — found a mix: "
            f"{ {t: ('primary_key' in TABLE_CONFS[t]) for t in FLAT_TABLES} }. Generalize "
            "build_flat_table's output: template deliberately to handle both cases (see "
            "feedback_build_rule_table_mixup memory before reintroducing per-table rule "
            "generation)."
        )
    FLAT_SUFFIX = "_array" if "primary_key" in TABLE_CONFS[FLAT_TABLES[0]] else ""

    for _t in FLAT_TABLES:
        _expected = f"{OUTPUT}/{_t}_{FLAT_YEAR_MIN}_{FLAT_YEAR_MAX}{FLAT_SUFFIX}.parquet"
        _actual = f"{OUTPUT}/" + (
            TABLE_CONFS[_t]["output_name"].format(year_min=FLAT_YEAR_MIN, year_max=FLAT_YEAR_MAX)
            if "output_name" in TABLE_CONFS[_t] else f"{_t}_{FLAT_YEAR_MIN}_{FLAT_YEAR_MAX}"
        ) + f"{FLAT_SUFFIX}.parquet"
        if _actual != _expected:
            raise ValueError(
                f"build_flat_table assumes every non-sharded table's output path follows "
                f"{{table}}_{{year_min}}_{{year_max}}[_array].parquet — '{_t}' resolves to "
                f"'{_actual}', not the expected '{_expected}'. This likely means '{_t}' has a "
                f"customized output_name in conf/build_tables/{DATASET}.yaml; either align it "
                "with this convention or generalize build_flat_table's output: template "
                "deliberately (see feedback_build_rule_table_mixup memory before reintroducing "
                "per-table rule generation)."
            )
    del _t, _expected, _actual

# ── Wildcard constraints ───────────────────────────────────────────────────────

wildcard_constraints:
    table = "|".join(re.escape(t) for t in TABLE_NAMES),
    year  = r"\d{4}",

# ── Helpers ────────────────────────────────────────────────────────────────────

def flat_table_path(table: str) -> str:
    """
    Mirrors build_tables.py's non-sharded write naming (a single flat file covering the whole
    year_min..year_max range). Sharded tables don't use this — see build_sharded_table below,
    which declares one flat file per (table, year) directly as static Snakemake output instead.
    """
    cfg   = TABLE_CONFS[table]
    y_min = cfg["year_min"]
    y_max = cfg["year_max"]
    suffix = "_array" if "primary_key" in cfg else ""
    if "output_name" in cfg:
        base = cfg["output_name"].format(year_min=y_min, year_max=y_max)
    else:
        base = f"{table}_{y_min}_{y_max}"
    return f"{OUTPUT}/{base}{suffix}.parquet"

# ── onstart ────────────────────────────────────────────────────────────────────

onstart:
    for d in ["logs/select_variables", "logs/build_tables", "logs/qc"]:
        Path(d).mkdir(parents=True, exist_ok=True)

# ── Rules ──────────────────────────────────────────────────────────────────────

rule all:
    input:
        [
            f"{OUTPUT}/{t}_{y}{SHARD_SUFFIX}.parquet"
            for t in SHARDED_TABLES
            for y in range(SHARD_YEAR_MIN, SHARD_YEAR_MAX + 1)
        ],
        [flat_table_path(t) for t in FLAT_TABLES],
        [
            f"{INTERMEDIATE}/{t}_{y}.qc/qc.json"
            for t in TABLE_NAMES
            for y in range(TABLE_CONFS[t]["year_min"], TABLE_CONFS[t]["year_max"] + 1)
        ],


rule select_variables:
    output:
        INTERMEDIATE + "/{table}_{year}.parquet",
    log:
        "logs/select_variables/{table}_{year}.log",
    params:
        dataset   = DATASET,
        datapaths = DATAPATHS,
    shell:
        r"""
        python run_select_variables.py \
            table_config={params.dataset}/{wildcards.table} \
            year={wildcards.year} \
            datapaths={params.datapaths} \
            > {log} 2>&1
        """


# run_build_tables.py processes ALL years for one table in a single invocation, writing all
# shard files at once (for sharded tables) or a single file (non-sharded). Deliberately kept
# this way rather than splitting sharded tables into one job per year (which was tried): loading
# every year together via read_parquet(..., union_by_name=True) in load_parquet_glob acts as an
# implicit schema-drift check across years — a column type change or rename in some year's
# Stage-1 output surfaces immediately, which per-year loading would silently miss since each
# year would never be compared against the others.
#
# Two rules below (sharded vs. non-sharded), NOT a per-table dynamically-named rule generated in
# a Python loop, which is what an earlier attempt here used. That loop pattern
# (rule: name: f"build_{_t}" ... inside `for _t in TABLE_NAMES:`) reliably caused a job for one
# table to silently execute a *different* table's entire resolved shell command whenever 2+
# build_* targets were requested in the same Snakemake session — confirmed via a diagnostic
# echo, and NOT fixed by making each rule's shell command a distinct wrapper-script file (ruling
# out shell-string similarity as the cause). select_variables/qc, which are genuinely
# wildcarded, never showed this bug. Root cause: almost certainly a Snakemake 8.4.8-specific
# local-scheduler/job-dispatch bug — the scheduler was substantially rewritten in 9.10.0
# ("migrate to scheduler plugin interface and scheduler plugins") and the bug did not reproduce
# locally on 9.23.1 using the exact same per-table-loop pattern. Upgrading Snakemake is the more
# direct fix if that's viable. See feedback_build_rule_table_mixup memory for the full
# diagnosis, including a `checkpoint`-based alternative (output was just a per-table completion
# marker, not the real files) and a per-(table, year) wildcarded alternative (reverted — it
# would have broken the all-years-in-one-pass schema-drift check described above) that were
# tried before this.
rule build_sharded_table:
    input:
        lambda wc: [
            f"{INTERMEDIATE}/{wc.table}_{y}.parquet" for y in range(SHARD_YEAR_MIN, SHARD_YEAR_MAX + 1)
        ]
    output:
        # Static (non-function) list — see SHARD_YEAR_MIN/MAX and SHARD_SUFFIX assertions above
        # for why this is safe: every sharded table matching {table} is asserted to share this
        # exact year range and aggregation status, so this list is valid regardless of which
        # table wildcard value a given job resolves to.
        [OUTPUT + "/{table}_" + str(y) + SHARD_SUFFIX + ".parquet" for y in range(SHARD_YEAR_MIN, SHARD_YEAR_MAX + 1)]
    log:
        "logs/build_tables/{table}.log"
    params:
        dataset   = DATASET,
        datapaths = DATAPATHS,
    wildcard_constraints:
        table = "|".join(re.escape(t) for t in SHARDED_TABLES),
    shell:
        "python run_build_tables.py "
        "dataset={params.dataset} table={wildcards.table} datapaths={params.datapaths} "
        "> {log} 2>&1"


# Non-sharded tables aggregate all years into a single file. Genuinely wildcarded on {table},
# same pattern as build_sharded_table above — not hand-written to a specific table name and not
# Python-loop-generated, so this stays correct regardless of how many non-sharded tables exist
# (today: just beneficiaries) or what they're named, as long as the FLAT_TABLES assertions above
# hold. Only reachable when FLAT_TABLES is non-empty (see wildcard_constraints below — an empty
# alternation would make the rule unmatchable, which is fine, just dead).
if FLAT_TABLES:
    rule build_flat_table:
        input:
            lambda wc: [
                f"{INTERMEDIATE}/{wc.table}_{y}.parquet" for y in range(FLAT_YEAR_MIN, FLAT_YEAR_MAX + 1)
            ]
        output:
            # Static (non-function) string — see the FLAT_TABLES assertions above for why this
            # is safe: every non-sharded table matching {table} is asserted to share this exact
            # year range, aggregation status, and output-naming convention.
            OUTPUT + "/{table}_" + str(FLAT_YEAR_MIN) + "_" + str(FLAT_YEAR_MAX) + FLAT_SUFFIX + ".parquet"
        log:
            "logs/build_tables/{table}.log"
        params:
            dataset   = DATASET,
            datapaths = DATAPATHS,
        wildcard_constraints:
            table = "|".join(re.escape(t) for t in FLAT_TABLES),
        shell:
            "python run_build_tables.py "
            "dataset={params.dataset} table={wildcards.table} datapaths={params.datapaths} "
            "> {log} 2>&1"


rule qc:
    input:
        INTERMEDIATE + "/{table}_{year}.parquet",
    output:
        INTERMEDIATE + "/{table}_{year}.qc/qc.json",
    log:
        "logs/qc/{table}_{year}.log",
    params:
        dataset   = DATASET,
        datapaths = DATAPATHS,
    shell:
        r"""
        python run_qc.py \
            table_config={params.dataset}/{wildcards.table} \
            year={wildcards.year} \
            datapaths={params.datapaths} \
            > {log} 2>&1
        """
