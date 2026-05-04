import io
import os
import json
import zstandard as zstd

PII_FILTER = {
    "LICENSE_PLATE": {"ces","deu","dan","ell","eng","spa","isl","tl","lvs","mlt","pol","por","ron","slv","swe","tur","ukr"},
    "PHONE_NUMBER": {"ces","dan","deu","eng","fin","ell","gle","isl","ita","kat","lvs","mkd","mlt","nld","pol","ron","slk","slv"},
    "GOVID": {"cat","glg","eus","ces","gle","eng","spa","fra","deu","hrv","hbs","hun","isl","lit","mkd","nld","nor","nno","nob"},
    "SOCIAL_INSURANCE": {"ell","eng","fra"},
}


def should_filter(entity_name: str, lang: str) -> bool:
    return entity_name in PII_FILTER and lang in PII_FILTER[entity_name]


def extract_lang_from_path(path: str) -> str:
    parts = path.split(os.sep)
    for p in parts:
        if "_" in p:
            lang = p.split("_")[0]
            if len(lang) in (2, 3):
                return lang
    return None


def transform_output_path(input_path: str) -> str:
    return input_path.replace("pii_todo", "pii", 1)


def process_jsonl_stream(reader, writer, lang):
    text_reader = io.TextIOWrapper(reader, encoding="utf-8")

    for line in text_reader:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        if not should_filter(record.get("name"), lang):
            writer.write((json.dumps(record) + "\n").encode("utf-8"))


def process_file(input_path: str):
    lang = extract_lang_from_path(input_path)
    if not lang:
        print(f"[WARN] Could not detect language for {input_path}, skipping")
        return

    output_path = transform_output_path(input_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    is_compressed = input_path.endswith(".zst") or input_path.endswith(".zstd")

    if is_compressed:
        dctx = zstd.ZstdDecompressor()
        cctx = zstd.ZstdCompressor()

        with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
            with dctx.stream_reader(fin) as reader:
                with cctx.stream_writer(fout) as writer:
                    process_jsonl_stream(reader, writer, lang)

    else:
        with open(input_path, "r", encoding="utf-8") as fin, \
             open(output_path, "w", encoding="utf-8") as fout:

            for line in fin:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if not should_filter(record.get("name"), lang):
                    fout.write(json.dumps(record) + "\n")

    print(f"[OK] {input_path} -> {output_path}")


def process_directory(root_dir: str):
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if (
                fname.endswith(".jsonl.zst") or
                fname.endswith(".jsonl.zstd") or
                fname.endswith(".jsonl")
            ):
                full_path = os.path.join(dirpath, fname)
                process_file(full_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Recursive PII filtering")
    parser.add_argument("--input_dir", required=True, help="Root folder (e.g., pii_folder/finepdfs-edu)")
    args = parser.parse_args()

    # /scratch/project_462000963/users/tudormateiu/pii_todo/nemotron

    process_directory(args.input_dir)
