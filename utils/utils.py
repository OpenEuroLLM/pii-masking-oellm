import polars as pl

def update_job_id(shards_dict: list, job: dict, which_job_id: str, job_id: str, shards_jsonl: str):
    # Lookup table using names
    shards_by_name = {row["name"]: row for row in shards_dict}

    # Add or update job id if job's name is same as in the original shards file
    if job["name"] in shards_by_name:
        shard_row = shards_by_name[job["name"]]
        shard_row[which_job_id] = job_id
    
    aux_df = pl.DataFrame(shards_dict)
    aux_df.write_ndjson(shards_jsonl)

def generate_job_name(shards_name: str, dataset_name: str = "", language: str = "") -> str:
    job_name = "nc-dc"

    if dataset_name:
        job_name += f"_{dataset_name}"
    
    if language:
        job_name += f"_{language}"
    
    job_name += f"_{shards_name}"

    return job_name

def update_job_id_array(which_job_id: str, which_status: str, job_id: str, shards_jsonl: str):
    shards_dict = pl.read_ndjson(shards_jsonl).to_dicts()

    # Lookup table using names
    row = shards_dict[int(job_id.split("_")[-1])-1]
    row[which_job_id] = job_id
    row[which_status] = "PENDING"
    
    aux_df = pl.DataFrame(shards_dict)
    aux_df.write_ndjson(shards_jsonl)