#!/bin/bash
#SBATCH --job-name=run_PE_time_coherence
#SBATCH --output=job_output_%j.txt
#SBATCH --error=job_error_%j.txt
#SBATCH --ntasks=30
#SBATCH --time=03:00:00
#SBATCH --mem=50G
#SBATCH --partition=ckpt

# Load modules required for conda
module load anaconda3

# Activate your conda environment
source activate kb

# Load environment variables from .env file
set -o allexport
source .env
set +o allexport

# Loop through each .nc file and submit a job
for dciw_filepath in ${data_directory}time_coherence_iws/*.nc; do
    sbatch --wrap="python simulation/monte_carlo_iws/run_PE_time_coherence.py LJ01C ${dciw_filepath}"
done