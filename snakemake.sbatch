#!/bin/bash
#SBATCH --job-name=normalizecms
#SBATCH --mem=200G    
#SBATCH --time=24:00:00
#SBATCH --partition=dominici-large
#SBATCH --output=logs/slurm-%j.out
#SBATCH --error=logs/slurm-%j.err

# Activate environment

# Run Snakemake
snakemake --unlock
snakemake --cores 2 --rerun-incomplete