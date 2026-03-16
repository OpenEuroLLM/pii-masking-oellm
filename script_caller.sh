#!/bin/bash


DATASET="nemotron"
LANGS=(
  # "bul_Cyrl"
  # "cat_Latn"
  # "ces_Latn"
  # "dan_Latn"
  # "deu_Latn"
  # "ell_Grek"
  "eng_Latn"
  # "eus_Latn"
  # "ekk_Latn"
  # "fin_Latn"
  # "fra_Latn"
  # "gle_Latn"
  # "glg_Latn"
  # "hrv_Latn"
  # "hun_Latn"
  # "ita_Latn"
  # "lit_Latn"
  # "lvs_Latn"
  # "nld_Latn"
  # "pol_Latn"
  # "por_Latn"
  # "ron_Latn"
  # "slk_Latn"
  # "slv_Latn"
  # "spa_Latn"
  # "swe_Latn"
  # "kat_Geor"
  # "als_Latn"
  # "srp_Cyrl"
  # "tur_Latn"
  # "ukr_Cyrl"
  # "isl_Latn"
)

for LANG in "${LANGS[@]}"; do
  python3 utils/status.py --shards-jsonl "generated_jobs/$DATASET/${DATASET}_${LANG}.jsonl"
done
