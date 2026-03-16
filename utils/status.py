
import json
import subprocess
import polars as pl
from pathlib import Path
import argparse
from loguru import logger

CODE_ELEGIBILITY = ["PENDING", "NOT_STARTED", "FAILED", "PREEMPTED", "SUSPENDED", None, "CANCELLED+", "TIMEOUT"]

def get_job_status(job_id: str) -> str:
    #logger.info(f"      Getting job status for job: {job_id}")

    # Get job information from sacct in JSON format
    try:
        cmd = ["sacct", "--jobs", job_id, "-n", "-X", "--format", "state"]
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"sacct failed for job {job_id}: {e}")
        raise RuntimeError(f"sacct failed: {e.stderr or e.stdout}") from e

    # Get only relevant information from the parsed job
    try:
        lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
        if not lines:
            return "SAME_STATUS"

        status = lines[0]

    except Exception as e:
        logger.error(f"Unexpected error when using parsed information for job {job_id}: {e}")
        raise RuntimeError(f"Unexpected error when using parsed information for job: {e}") from e

    #logger.info(f"      Job status: {status}")
    return status

def get_job_status_json(job_id: str) -> str:
    logger.info(f"      Getting job status for job: {job_id}")

    # Get job information from sacct in JSON format
    try:
        cmd = ["sacct", "--jobs", job_id, "--json"]
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        
    except subprocess.CalledProcessError as e:
        logger.error(f"sacct failed for job {job_id}: {e}")
        raise RuntimeError(f"sacct failed: {e.stderr or e.stdout}") from e

    # Parse JSON output from sacct
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse sacct JSON output for job {job_id}: {e}")
        raise RuntimeError("Failed to parse sacct JSON output") from e

    # Get only relevant information from the parsed job
    try:
        job = payload.get("jobs")[0]
        status = job["state"]["current"]
    except Exception as e:
        logger.error(f"Unexpected error when using parsed information for job {job_id}: {e}")
        raise RuntimeError(f"Unexpected error when using parsed information for job: {e}") from e

    logger.info(f"      Job status: {status}")
    return status

def update_all_job_status(shards_jsonl: Path):
    logger.info(f"Updating job status for all jobs...")

    shards_dict = pl.read_ndjson(shards_jsonl).to_dicts()

    # Iterate over all shard groups, get their id and status,
    # and update shard group status when relevant
    results = []
    for i, shard_group in enumerate(shards_dict):
        #logger.info(f"{i} - Checking job: {shard_group['name']}")
        if "job_id" in shard_group:
            shards_id = shard_group["job_id"]
            shards_status = shard_group["status"]

            # Check if job id has a value
            if shards_id:
                job_status = get_job_status(shards_id)

                if job_status == "SAME_STATUS":
                    job_status = shard_group["status"]
                
                if job_status in CODE_ELEGIBILITY:
                    logger.info(f" - PII job <{shard_group['name']}> has status <{job_status}>!")

                if shards_status != job_status:
                    logger.info(f" - PII job <{shard_group['name']}> status changed from <{shards_status}> to <{job_status}>")
                    shard_group["status"] = job_status
                
                results.append(job_status)
    aux_df = pl.DataFrame(shards_dict)
    aux_df.write_ndjson(shards_jsonl)

    if len(results) == len(shards_dict) and all([result == "COMPLETED" for result in results]):
        logger.info(f"-----")
        logger.info(f"- PII jobs <COMPLETED>!")
    else:
        logger.info(f"-----")
        logger.info(f"- PII jobs <REMAINING>!")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards-jsonl", type=Path, required=True, help="JSONL file containing job information of dataset shards.")
    args = parser.parse_args()

    update_all_job_status(args.shards_jsonl)


if __name__ == "__main__":
    main()
