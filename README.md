# pii-masking-oellm



## Installation

Step-by-step instructions to get the project running on Lumi HPC:

1. Install repository on Lumi login node:
   ```bash
   # Load Python 3.11.7
   module load cray-python/3.11.7
   python --version

   # Clone repo
   mkdir pii
   cd pii
   git clone https://github.com/OpenEuroLLM/pii-masking-oellm.git

   # Create a virtual environment
   python3 -m venv venv
   source venv/activate/bin

   # Install the local requirements
   cd pii-masking-oellm
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

2. Build Singularity container outside Lumi:

   ```bash
   # Clone repo
   git clone https://github.com/OpenEuroLLM/pii-masking-oellm.git
   
   # Clone PII tool repo
   git clone https://github.com/mmanteli/multilingual-PII-tool.git

   # Important! There is a bug in the Makefile in the multilingual-PII-tool
   # Line 9 in the Makefile must be changed from this:
   # 
   # NAME := pii-manager
   #
   # To this:
   #
   # NAME := pii_manager


   # Build container
   singularity build --fakeroot pii_oellm.sif pii_oellm.def
   ```

   The built Singularity container needs to be copied inside the root path of the repository from step 1. E.g. `/users/YOUR_USER/pii/pii-masking-oellm/pii_oellm.sif`

## Usage

First step before starting to use the tool is to prepare all the necessary folders and environment variables.

1. Scratch user folder and PII masking output folder:

   ```bash
   mkdir -p /scratch/YOUR_PROJECT_ID/users/YOUR_USER/pii
   mkdir -p /scratch/YOUR_PROJECT_ID/users/YOUR_USER/tmp
   ```

2. Edit the `env_variables.yaml` file with the needed information:

    ```yaml
    PII_DIR: "/scratch/YOUR_PROJECT_ID/users/YOUR_USER/pii"
    DATASETS_DIR: "/appl/local/openeurollm/training/catalogue"
    USER_NAME: "YOUR_USER"
    PROJECT_ID: "YOUR_PROJECT_ID"
    LANGS:
    - eng_Latn
    - bul_Cyrl
    - hrv_Latn
    - ces_Latn
    - dan_Latn
    ...
    ```

    The `LANGS` list currently available in the file is the complete list of languages encompassed for PII masking, so there is no need to edit it unless more are added or removed later on.

Next step before submitting any jobs is the preparation of `.jsonl` files that keep track, for each dataset, what shards still need to be submitted for processing. These files keep track of jobs completed, failed, submitted, ...

   ```bash
   # Create job tracking .jsonl for e.g. 'nemotron'
   python3 create_jobs.py datasets_info/nemotron.yaml --output jobs/
   ```

This step creates `.jsonl` files for as many languages are supported in the dataset. The files contain lines like these:

   ```json
   {"status": null, "job_id": null, "dataset_name": "nemotron-cc_eng_Latn", "path": "nemotron-cc/1.0/high/actual/CC-MAIN-2018-13-part-00011.jsonl.zstd"}
   ```

where:
- `status`: job status in slurm
- `job_id`: job id in slurm
- `dataset_name`: dataset name + language
- `path`: relative path of the shard, used for later ingestion and to keep the same folder structure in the output 

After completing required setup, the tool can be invoked to submit jobs to the system. Example job submission:

   ```bash
   python3 submitter.py \
      --shards-jsonl generated_jobs/nemotron/nemotron_eng_Latn.jsonl \
      --partition small \
      --job-limit 1 \
      --time 4:00:00 \
      --mem 224G \
      --cpus 128 \
      --lang en \
      --id-field warc_record_id \
      --pii-mode extract
   ```

Allowed parameters for job submission Python script:

| Parameter | Type | Required | Default | Description |
|-----------|------|---------|---------|-------------|
| `--shards-jsonl` | Path | Yes | — | JSONL file containing job information for the created shards. |
| `--partition` | str | No | `small` | Lumi partition to be used. |
| `--job-limit` | int | No | `1` | Limit on how many jobs will be submitted together. |
| `--time` | str | No | `4:00:00` | Time limit per job submitted. |
| `--mem` | str | No | `224G` | Amount of memory to use. |
| `--cpus` | int | No | `128` | CPUs per task to use. |
| `--lang` | str | Yes | — | ISO 639-1 2-character language code, e.g., `en`, `es`. |
| `--id-field` | str | Yes | — | Name of ID field for the dataset being processed. |
| `--metadata-field` | str | No | `""` | Specific metadata field where document IDs are, if present. E.g., DCLM uses `metadata`. |
| `--pii-mode` | str | No | `extract` | Tool’s mode of PII processing. Allowed modes: `full`, `extract`, `replace`. |
