#!/bin/bash
# Plano B pra apresentacao -- se o Airflow UI der problema no dia da demo,
# roda o pipeline inteiro direto via spark-submit, sem depender da DAG.
#
# Uso:
#   ./scripts/rodar_pipeline_local.sh 2023-12-01
#   ./scripts/rodar_pipeline_local.sh 2023-12-01 2023-12-08   (processa o intervalo todo)
set -e

DATA_INICIO="${1:?Uso: rodar_pipeline_local.sh <data-inicio> [data-fim]}"
DATA_FIM="${2:-$DATA_INICIO}"

echo "=== ShopBrasil Pipeline -- execucao manual (plano B) ==="
echo "Periodo: $DATA_INICIO a $DATA_FIM"
echo

data_atual="$DATA_INICIO"
while [ "$(date -d "$data_atual" +%s)" -le "$(date -d "$DATA_FIM" +%s)" ]; do
  echo "--- Processando $data_atual ---"

  docker exec shopbrasil-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --executor-memory 1g --driver-memory 1g \
    /opt/spark-jobs/ingestao.py --data-ref "$data_atual" \
    --input-path /opt/airflow/data/raw --output-path /opt/airflow/data/datalake

  docker exec shopbrasil-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --executor-memory 1g --driver-memory 1g \
    /opt/spark-jobs/transformacao.py --data-ref "$data_atual" \
    --output-path /opt/airflow/data/datalake

  docker exec shopbrasil-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --executor-memory 1g --driver-memory 1g \
    /opt/spark-jobs/agregacao.py --data-ref "$data_atual" \
    --input-path /opt/airflow/data/raw --output-path /opt/airflow/data/datalake

  echo "--- $data_atual concluido ---"
  echo

  data_atual=$(date -d "$data_atual + 1 day" +%Y-%m-%d)
done

echo "=== Pipeline concluido para o periodo $DATA_INICIO a $DATA_FIM ==="
echo "Confira o resultado em: data/datalake/gold/"
