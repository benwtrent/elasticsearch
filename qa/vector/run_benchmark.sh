#!/bin/bash
set -euo pipefail

export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/gcloud/application_default_credentials.json"

ES_DIR="/home/benjamintrent/elasticsearch"
DATA_DIR="/mnt/data/.data"
INDEX_DIR="/mnt/data/knn_index"
LOG_DIR="/mnt/data/logs"
CSV_PATH="/mnt/data/all_results"
COMPLETED_FILE="/mnt/data/all_results.txt"
CONFIG_FILE="/tmp/bench_config.json"

DATASETS=("dbpedia-entity-gte-base" "dbpedia-entity-E5-small" "hotpotqa-gte-base" "hotpotqa-E5-small")
IVF_CLUSTER_SIZES=(16 32 64 128 256 512 1024)
SECONDARY_CLUSTER_SIZES=(16 32 64 128 256 512)
QUANTIZE_BITS=(1 2 4)
CENTROID_HNSW_INDEXED=(false true)

mkdir -p "$DATA_DIR" "$INDEX_DIR" "$LOG_DIR"
mkdir -p "$ES_DIR/qa/vector/target"
ln -sfn "$INDEX_DIR" "$ES_DIR/qa/vector/target/knn_index"
touch "$COMPLETED_FILE"

cd "$ES_DIR"

total_combos=$(( ${#DATASETS[@]} * ${#IVF_CLUSTER_SIZES[@]} * ${#SECONDARY_CLUSTER_SIZES[@]} * ${#QUANTIZE_BITS[@]} * ${#CENTROID_HNSW_INDEXED[@]} ))
current=0

for dataset in "${DATASETS[@]}"; do
  for ivf_size in "${IVF_CLUSTER_SIZES[@]}"; do
    for sec_size in "${SECONDARY_CLUSTER_SIZES[@]}"; do
      current=$((current + 1))
      run_key="${dataset}_ivf${ivf_size}_sec${sec_size}_quant${QUANTIZE_BITS}_hnsw${CENTROID_HNSW_INDEXED}"

      if grep -qF "$run_key" "$COMPLETED_FILE"; then
        echo "[$current/$total_combos] SKIP (already done): $run_key"
        continue
      fi

      echo "[$current/$total_combos] START: $run_key"
      log_file="$LOG_DIR/${run_key}.log"

      cat > "$CONFIG_FILE" <<EOCONFIG
[
  {
    "dataset": "$dataset",
    "data_dir": "$DATA_DIR",
    "num_queries": 400,
    "quantize_bits": $QUANTIZE_BITS,
    "k": [100],
    "index_type": "ivf",
    "centroids_indexed": $CENTROID_HNSW_INDEXED,
    "ivf_cluster_size": $ivf_size,
    "secondary_cluster_size": $sec_size,
    "index_threads": 16,
    "merge_workers": 16,
    "num_searchers": 8,
    "search_threads": 8,
    "reindex": true,
    "force_merge": false,
    "visit_percentage": [0.025, 0.025, 0.025, 0.025, 0.5,0.5,0.5,0.5,0.5, 1,1,1,1,1, 2,2,2,2,2, 2.5,2.5,2.5,2.5,2.5, 3,3,3,3,3, 4,4,4,4,4, 5,5,5,5,5],
    "over_sampling_factor": [1.0, 3.0, 5.0],
    "vector_space": "dot_product"
  }
]
EOCONFIG

      if ./gradlew :qa:vector:checkVec --args="$CONFIG_FILE --csv=$CSV_PATH" \
            > "$log_file" 2>&1; then
        echo "$run_key" >> "$COMPLETED_FILE"
        echo "[$current/$total_combos] DONE: $run_key"
      else
        echo "[$current/$total_combos] FAILED: $run_key (see $log_file)"
      fi

      rm -rf "$INDEX_DIR"/*
    done
  done
done

echo ""
echo "========================================="
echo "Benchmark complete (HNSW centroids)."
echo "Index CSV:  ${CSV_PATH}_index.csv"
echo "Search CSV: ${CSV_PATH}_search.csv"
echo "========================================="
