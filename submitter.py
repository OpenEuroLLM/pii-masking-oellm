
import os
import json
import yaml
import time
import shutil
import datetime
import argparse
import subprocess
import polars as pl
from pathlib import Path
from loguru import logger
from utils.utils import update_job_id_array

CODE_ELEGIBILITY = ["NOT_STARTED", "FAILED", "PREEMPTED", "SUSPENDED", None, "CANCELLED+", "TIMEOUT"]
WORK_DIRECTORY = None
ACCOUNT_ID = None
USER_NAME = None
CATALOGUE_DIR = None
with open('env_variables.yaml', 'r') as file:
    variables = yaml.safe_load(file)
    WORK_DIRECTORY = variables["PII_DIR"]
    ACCOUNT_ID = variables["PROJECT_ID"]
    USER_NAME = variables["USER_NAME"]
    CATALOGUE_DIR = variables["DATASETS_DIR"]
    Path(WORK_DIRECTORY).mkdir(exist_ok=True, parents=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards-jsonl", type=Path, required=True,
                        help="JSONL file containing job information for the created shards.")
    parser.add_argument("--partition", type=str, default="small", 
                        help="Lumi partition to be used.")
    parser.add_argument("--job-limit", type=int, default=1, 
                        help="Limit on how many jobs will be submitted together.")
    parser.add_argument("--time", type=str, default="4:00:00", 
                        help="Time limit per job submitted.")
    parser.add_argument("--mem", type=str, default="224G", 
                        help="Amount of memory to use.")
    parser.add_argument("--cpus", type=int, default=128, 
                        help="CPUs per task to use.")
    parser.add_argument("--lang", type = str, required = True, 
                        help = "ISO 639-1 2-char language code. E.g.: 'en', 'es'")
    parser.add_argument("--id-field", type = str, required = True,
                        help = "Name of ID field for the dataset being processed.")
    parser.add_argument("--metadata-field", type = str, default = "", 
                        help = "Specific metadata field where document \
                        IDs are, if there is one. E.g. DCLM has 'metadata'.")
    parser.add_argument("--pii-mode", type = str, 
                        default = "extract",
                        choices = ["full", "extract", "replace"],
                        help = "Tool's mode of PII processing. Allowed modes: \
                            'full', 'extract', 'replace'")
    args = parser.parse_args()
    
    Path("logs", "pii_extraction").mkdir(exist_ok=True, parents=True)
    logger.add(f"logs/pii_extraction/extraction_{args.shards_jsonl}_{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.log")
    
    # Prepare sbatch array indices for as many jobs as are allowed 
    # or as many jobs as are still available.
    shards_dict = pl.read_ndjson(args.shards_jsonl).to_dicts()
    jobs_to_submit = []
    dataset_name = ""
    for idx, shard_group in enumerate(shards_dict):
        if shard_group["status"] in CODE_ELEGIBILITY:
            actual_idx = str(idx + 1)
            jobs_to_submit.append(actual_idx)
            dataset_name = shard_group["dataset_name"]
            if len(jobs_to_submit) >= args.job_limit:
                break

    # Create output dir and subdirectories just in case
    logs_path = Path(WORK_DIRECTORY, dataset_name, "logs", "pii")
    Path(logs_path).mkdir(parents=True, exist_ok=True)
    pii_extracted_dir = Path(WORK_DIRECTORY, dataset_name, "pii_extracted")
    Path(pii_extracted_dir).mkdir(parents=True, exist_ok=True)

    # Write back the jsonl with the updated shards that have started
    shards_df = pl.DataFrame(shards_dict)
    shards_df.write_ndjson(args.shards_jsonl)
    time.sleep(5)

    logger.info(f"_____________")
    logger.info(f"PARAMETERS")
    logger.info(f"- Logs directory: {logs_path}")
    logger.info(f"- PII output directory: {pii_extracted_dir}")
    logger.info(f"- Shards jsonl: {args.shards_jsonl}")
    logger.info(f"- Job limit: {args.job_limit}")
    logger.info(f"- Account: {ACCOUNT_ID}")
    logger.info(f"- Partition: {args.partition}")
    logger.info(f"- Time limit: {args.time}")
    logger.info(f"- Number workers: {args.cpus}")
    logger.info(f"- Memory: {args.mem}")
    logger.info(f"- PII language: {args.lang}")
    logger.info(f"- PII mode: {args.pii_mode}")
    logger.info(f"- Dataset ID field: {args.id_field}")
    logger.info(f"- Metadata field: {args.metadata_field}")
    logger.info(f"_____________")
    logger.info(f"Finding jobs to run...")

    if jobs_to_submit:
        logger.info(f"Found {len(jobs_to_submit)} jobs...")
    else:
        logger.info("No jobs to submit.")
        return
    
    logger.info(f"_____________")
    logger.info(f"- Submitting jobs to the system...")

    # Set some required slurm variables
    array_idxs = ",".join(jobs_to_submit)
    job_name = f"{dataset_name}_%A_%a"
    output_logs = f"{logs_path}/job_%A_%a.out"
    error_logs = f"{logs_path}/job_%A_%a.err"
    slurm_script_path = "utils/pii_job_template.slurm"
    
    # Create sbatch command with the specified slurm parameters,
    # and the specified PII tool parameters
    cmd = ["sbatch", "--array", f"{array_idxs}", 
            "--job-name", job_name,
            "--account", ACCOUNT_ID,
            "--partition", args.partition,
            "--cpus-per-task", str(args.cpus),
            "--mem", args.mem,
            "--time", args.time,
            "--output", output_logs,
            "--error", error_logs,
            slurm_script_path,
            args.shards_jsonl,
            pii_extracted_dir,
            args.lang,
            args.metadata_field,
            args.id_field,
            args.pii_mode,
            CATALOGUE_DIR
            ]

    # Run sbatch command via subprocess and catch errors
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"sbatch failed for job {job_name}: {e}")
        raise RuntimeError(f"sbatch failed: {e.stderr or e.stdout}") from e

    # Write back to jsonl the updated jobs with their respective ID and status
    sbatch_job_id = result.stdout.strip().split()[-1]
    time.sleep(15)
    for idx in jobs_to_submit:
        job_id = f"{sbatch_job_id}_{idx}"
        update_job_id_array(which_job_id = "job_id",
                                which_status = "status",
                                job_id = job_id,
                                shards_jsonl = args.shards_jsonl)

    logger.info(f"- Submitted...")

if __name__ == "__main__":
    main()