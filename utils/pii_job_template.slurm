#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1

set -euo pipefail

echo "Running on node: $HOSTNAME"

JSONL_PATH=$1
OUTPUT_PII_DIR=$2
LANG=$3
METADATA_FIELD=$4
ID_FIELD=$5
PII_MODE=$6
CATALOGUE_DIR=$7

# Set thread amount from available CPUs to distribute
# to the pii pipeline and to the zstdmt ouputting.
# Main PII processing will use 2/3 of available CPUS,
# while output compression the remaining ones minus an overhead.
PROCESS_THREADS=$((SLURM_CPUS_PER_TASK * 3 / 4))
COMPRESS_THREADS=$((SLURM_CPUS_PER_TASK - PROCESS_THREADS - 2))

# Extract specific line from jsonl with shards to get
# the information left for the job
LINENUMBER=$SLURM_ARRAY_TASK_ID
line=$(sed -n "${LINENUMBER}p" "$JSONL_PATH")
echo "Reading line number $LINENUMBER: $line"

# Extract fields using jq using the line
SPLIT_FILE=$(echo "$line" | jq -r ".path")

echo "Shard list file: $SPLIT_FILE"

while IFS= read -r RELATIVE_INPUT_FILE_PATH; do

	# Set the input and output files so that both files are named
	# the same but input is taken from catalogue and output
	# is saved to the user's scratch folder
	INPUT_FILE_JSONL="$CATALOGUE_DIR/$RELATIVE_INPUT_FILE_PATH"
	OUTPUT_FILE_JSONL="$OUTPUT_PII_DIR/$RELATIVE_INPUT_FILE_PATH"

	# Create the folder structure up to the point of the output file
	mkdir -p "$(dirname "$OUTPUT_FILE_JSONL")"

	# Run PII tool processing
	# Runs via a singularity container which uses gnu parallel
	zstdcat "$INPUT_FILE_JSONL" \
	| srun /usr/bin/time -v singularity exec pii_oellm.sif \
	parallel --pipe -k \
		--halt soon,fail=1 \
		-j "$THREADS" --block 50M \
	python3 ./pii_tool.py \
		--lang "$LANG"  \
		--metadata-field "$METADATA_FIELD"  \
		--id-field "$ID_FIELD"  \
		--pii-mode "$PII_MODE"  \
	| zstdmt -T"$THREADS" -10 > "$OUTPUT_FILE_JSONL"
done < "$SPLIT_FILE"
