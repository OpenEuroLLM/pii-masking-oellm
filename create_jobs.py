import os
import json
import yaml
import argparse
from pathlib import Path

POSSIBLE_LANGS = None
CATALOGUE_DIR = None
with open("env_variables.yaml", "r") as file:
    variables = yaml.safe_load(file)
    POSSIBLE_LANGS = variables["LANGS"]
    CATALOGUE_DIR = Path(variables["DATASETS_DIR"])

def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def collect_files(dataset_path, directories, lang=None):
    dataset_root = CATALOGUE_DIR / dataset_path
    files = []
    
    for d in directories:
        if "{lang}" in d and lang:
            d = d.replace("{lang}", lang)

        search_dir = dataset_root / d
        if not search_dir.exists():
            continue

        for root, _, filenames in os.walk(search_dir):
            for fname in filenames:
                if fname.endswith(".jsonl.zst") or fname.endswith(".jsonl.zstd"):
                    full_path = Path(root) / fname
                    rel_path = full_path.relative_to(CATALOGUE_DIR)
                    files.append(str(rel_path))

    return files

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_config", help="Path to dataset YAML config")
    parser.add_argument("--output", default="jobs", help="Output folder for JSONL files")
    args = parser.parse_args()

    config = load_config(args.yaml_config)

    dataset_name = config["dataset_name"]
    dataset_path = config["dataset_path"]
    directories = config["directories"]

    output_dir = Path(args.output, dataset_name)
    output_dir.mkdir(parents=True, exist_ok=True)

    if "lang" in config:
        langs = [config["lang"]]
    else:
        langs = POSSIBLE_LANGS

    total_jobs = 0

    for lang in langs:
        files = collect_files(dataset_path, directories, lang)

        if not files:
            continue

        name = f"{dataset_path}_{lang}"
        output_file = output_dir / f"{dataset_path}_{lang}.jsonl"

        with open(output_file, "w") as f:
            for p in files:
                row = {
                    "status": None,
                    "job_id": None,
                    "dataset_name": name,
                    "path": p
                }
                f.write(json.dumps(row) + "\n")

        print(f"{output_file}: {len(files)} jobs")
        total_jobs += len(files)

    print(f"Total jobs written: {total_jobs}")


if __name__ == "__main__":
    main()