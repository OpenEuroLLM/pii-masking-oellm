#!/bin/bash
#SBATCH --job-name=pii_postprocess
#SBATCH --nodes=1
#SBATCH --partition=small
#SBATCH --cpus-per-task=128
#SBATCH --time=01:00:00
#SBATCH --mem=440G
#SBATCH --account=project_465002530
#SBATCH --output=/scratch/project_462000963/users/tudormateiu/tmp/general_logs/pii_general_job_%j.out
#SBATCH --error=/scratch/project_462000963/users/tudormateiu/tmp/general_logs/pii_general_job_%j.err

set -euo pipefail

python3 utils/pii_postfiltering.py --input_dir /scratch/project_462000963/users/tudormateiu/pii_todo/nemotron
