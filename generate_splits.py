import os
import argparse
from pathlib import Path
import yaml
import glob


DATASETS_DIR = None
WORK_DIRECTORY = None
POSSIBLE_LANGS = None
with open("env_variables.yaml", "r") as file:
    variables = yaml.safe_load(file)
    WORK_DIRECTORY = variables["PII_DIR"]
    Path(WORK_DIRECTORY).mkdir(exist_ok=True, parents=True)
    DATASETS_DIR = variables["DATASETS_DIR"]
    POSSIBLE_LANGS = variables["LANGS"]


def chunk_list(lst, size):
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def get_file_chunks(dir_path, shard_count):
    FORMATS = [".jsonl.zst", ".jsonl.zstd"]
    files = [
        str(p)
        for p in Path(dir_path).rglob("*")
        if p.is_file() and str(p).endswith(tuple(FORMATS)) and "counts" not in p.name
    ]
    files.sort()
    return chunk_list(files, shard_count)


def generate_stuff(dataset_path, input_dir, shard_count, output_dir):
    if not os.path.exists(input_dir) or len(os.listdir(input_dir)) == 0:
        raise ValueError(f"Directory {input_dir} does not exist or empty.")

    print("Generating chunks for:", input_dir)
    name_for_splits = str(input_dir).replace(
        DATASETS_DIR + "/" + dataset_path + "/", ""
    )
    # Ensure output directory exists
    final_output_dir = Path(output_dir, name_for_splits)
    final_output_dir.mkdir(parents=True, exist_ok=True)

    chunks = get_file_chunks(input_dir, shard_count)
    print(f"Total chunks: {len(chunks)}")

    # Write each chunk to a file
    for i, chunk in enumerate(chunks):
        final_name = name_for_splits.replace("/", "_")
        filename = os.path.join(final_output_dir, f"{final_name}_{i}.txt")
        with open(filename, "w") as f:
            for path in chunk:
                path = path.replace(DATASETS_DIR, "")
                f.write(path + "\n")
        print(f"Generated {filename}")


def main():
    parser = argparse.ArgumentParser(
        description="Split directory file list into chunk files."
    )
    parser.add_argument("--yaml-config", 
                        required=True, 
                        help="Dataset yaml configuration file.")
    parser.add_argument("--output-dir", 
                        required=True, 
                        help="Output directory for chunk files.")

    args = parser.parse_args()
    datasets_file = args.yaml_config
    output_dir = args.output_dir
    input_dir = Path(WORK_DIRECTORY, args.yaml_config)

    with open(datasets_file, "r") as file:
        splits = yaml.safe_load(file)
    for key, value in splits.items():
        directories = []
        for directory in value["directories"]:
            if "{lang}" in directory:
                directories.extend(
                    directory.replace("{lang}", lang) for lang in POSSIBLE_LANGS
                )
            else:
                directories.append(directory)
        splits[key]["directories"] = directories

    for dataset_name, dataset_info in splits.items():
        shard_count = dataset_info["shard_count"]
        dataset_path = dataset_info["dataset_path"]
        for input_dir in dataset_info["directories"]:
            full_input_dir = Path(DATASETS_DIR, dataset_path, input_dir)
            if full_input_dir.exists():
                full_output_dir = Path(output_dir, dataset_name)
                generate_stuff(
                    dataset_path, full_input_dir, shard_count, full_output_dir
                )
            else:
                print(f"Language {full_input_dir} does not exist")


if __name__ == "__main__":
    main()
