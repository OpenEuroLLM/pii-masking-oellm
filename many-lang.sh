#!/usr/bin/env bash

export PARTITION=debug
export DS=finepdfs
export ID_FIELD=id

ROWS=(
"bul_Cyrl	bg"
"ces_Latn	cs"
"dan_Latn	da"
"deu_Latn	de"
"ell_Grek	el"
"eng_Latn	en"
"est_Latn	et"
"ekk_Latn	et"
"fin_Latn	fi"
"fra_Latn	fr"
"gle_Latn	ga"
"hrv_Latn	hr"
"hun_Latn	hu"
"ita_Latn	it"
"lav_Latn	lv"
"ltg_Latn	lv"
"lvs_Latn	lv"
"lit_Latn	lt"
"mlt_Latn	mt"
"nld_Latn	nl"
"pol_Latn	pl"
"por_Latn	pt"
"ron_Latn	ro"
"slk_Latn	sk"
"slv_Latn	sl"
"spa_Latn	es"
"swe_Latn	sv"
"cat_Latn	ca"
"eus_Latn	eu"
"glg_Latn	gl"
"bos_Latn	bs"
"kat_Geor	ka"
"mkd_Cyrl	mk"
"sqi_Latn	sq"
"als_Latn	sq"
"srp_Cyrl	sr"
"srp_Latn	sr"
"tur_Latn	tr"
"ukr_Cyrl	uk"
"isl_Latn	is"
"nor_Latn	no"
"nno_Latn	no"
"nob_Latn	no"
)

for r in "${ROWS[@]}"; do
  set -- $r
  echo "iso3 $1 iso2 $2"
  python3 submitter.py --shards-jsonl generated_jobs/$DS/$DS_$1.jsonl --lang $2 --partition $PARTITION --job-limit 10 --time 0:50:00 --mem 224G --cpus 128 --id-field $ID_FIELD --pii-mode extract
done
