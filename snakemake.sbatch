#!/bin/bash
#SBATCH --job-name=normalizecms
#SBATCH --mem=400G    
#SBATCH --cpus-per-task=8
#SBATCH --time=2:00:00
#SBATCH --partition=dominici-xl
#SBATCH --output=logs/slurm-%j.out
#SBATCH --error=logs/slurm-%j.err

# Activate environment

# Run Snakemake
snakemake --unlock
snakemake --cores 1 --rerun-incomplete --forceall
