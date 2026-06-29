#Snakefile
import yaml
from pathlib import Path
configfile: "conf/snakemake.yaml"

# ── Load pipeline config ───────────────────────────────────────────────────────

DATASET   = config["dataset"]
DATAPATHS = config["datapaths"]

with open(f"conf/datapaths/{DATAPATHS}.yaml") as f:
    _dp = yaml.safe_load(f)

with open(f"conf/build_tables/{DATASET}.yaml") as f:
    _bt = yaml.safe_load(f)

INTERMEDIATE = _dp["dirs"]["intermediate"]
OUTPUT       = _dp["dirs"]["output"]
TABLES       = _bt["tables"]

# ── Wildcard constraints ───────────────────────────────────────────────────────

wildcard_constraints:
    year     = r"\d{4}",
    year_min = r"\d{4}",
    year_max = r"\d{4}",

# ── Helpers ────────────────────────────────────────────────────────────────────

def hydra_value(value):
    if isinstance(value, list):
        quoted = ", ".join([f'"{v}"' for v in value])
        return f"[{quoted}]"
    if value is None:
        return "null"
    return str(value)

def stage2_path(table):
    cfg  = TABLES[table]
    name = cfg["output_name"].format(year_min=cfg["year_min"], year_max=cfg["year_max"])
    return f"{OUTPUT}/{name}.parquet"

# ── onstart ────────────────────────────────────────────────────────────────────

onstart:
    for d in ["logs/hydra/select_variables", "logs/hydra/build_tables", "logs/hydra/qc"]:
        Path(d).mkdir(parents=True, exist_ok=True)


rule all:
    input:
        [stage2_path(t) for t in TABLES],
        [
            f"{INTERMEDIATE}/{table}_{year}.qc/qc.json"
            for table, cfg in TABLES.items()
            for year in range(cfg["year_min"], cfg["year_max"] + 1)
        ],

rule select_variables:
    output:
        INTERMEDIATE + "/{table}_{year}.parquet",
    log:
        "logs/select_variables/{table}_{year}.log",
    params:
        dataset   = DATASET,
        datapaths = DATAPATHS,
        hydra_dir = lambda wc: f"logs/hydra/select_variables/{wc.table}/{wc.year}",
    shell:
        r"""
        python run_select_variables.py \
            table_config={params.dataset}/{wildcards.table} \
            year={wildcards.year} \
            datapaths={params.datapaths} \
            hydra.run.dir="{params.hydra_dir}" \
            hydra.job.name="select_variables_{wildcards.table}_{wildcards.year}" \
            > {log} 2>&1
        """

rule build_tables:
    input:
        lambda wildcards: [
            f"{INTERMEDIATE}/{wildcards.table}_{year}.parquet"
            for year in range(
                TABLES[wildcards.table]["year_min"],
                TABLES[wildcards.table]["year_max"] + 1,
            )
        ],
    output:
        OUTPUT + "/{table}_{year_min}_{year_max}.parquet",
    log:
        "logs/build_tables/{table}_{year_min}_{year_max}.log",
    params:
        dataset   = DATASET,
        datapaths = DATAPATHS,
        hydra_dir = lambda wc: f"logs/hydra/build_tables/{wc.table}",
    shell:
        r"""
        python run_build_tables.py \
            dataset={params.dataset} \
            table={wildcards.table} \
            datapaths={params.datapaths} \
            hydra.run.dir="{params.hydra_dir}" \
            hydra.job.name="build_tables_{wildcards.table}" \
            > {log} 2>&1
        """

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
        hydra_dir = lambda wc: f"logs/hydra/qc/{wc.table}/{wc.year}",
    shell:
        r"""
        python run_qc.py \
            table_config={params.dataset}/{wildcards.table} \
            year={wildcards.year} \
            datapaths={params.datapaths} \
            hydra.run.dir="{params.hydra_dir}" \
            hydra.job.name="qc_{wildcards.table}_{wildcards.year}" \
            > {log} 2>&1
        """