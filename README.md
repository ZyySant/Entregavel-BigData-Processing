# ShopBrasil — Pipeline de Vendas

Projeto final da disciplina Big Data Processing (MBA Engenharia de Dados,
Mackenzie). Pipeline de dados end-to-end pra ShopBrasil (cliente fictício da
DataFlow Analytics, a empresa usada como narrativa do curso): ingestão
multi-formato, arquitetura Medallion (Bronze/Silver/Gold), qualidade de dados
customizada com quarentena, e orquestração via Airflow — tudo containerizado.

## Integrantes

| Nome | RA |
|---|---|
| _(preencher)_ | _(preencher)_ |
| _(preencher)_ | _(preencher)_ |
| _(preencher)_ | _(preencher)_ |

## Como rodar

Requisitos: Docker Desktop com Docker Compose v2, 8GB RAM livres, 4 cores.

```bash
docker compose up -d --build
```

A primeira subida demora um pouco mais (build da imagem do Airflow + pull do
Spark). Acompanhe com `docker compose ps` até todos os serviços ficarem
`healthy` ou `running`.

- Spark Master UI: http://localhost:8080
- Airflow UI: http://localhost:8081 (usuário `admin`, senha `admin`)

Na UI do Airflow, ative a DAG `shopbrasil_pipeline_vendas` (toggle à esquerda)
e dispare uma execução manual (botão de play) escolhendo a data
`2023-12-08` — é o dia com dados problemáticos, então dá pra ver o quality
gate e a quarentena funcionando de verdade. Os outros dias (01 a 07/12) têm
dados limpos.

Resultado final em `data/datalake/gold/`.

### Plano B (se o Airflow travar na hora da demo)

```bash
./scripts/rodar_pipeline_local.sh 2023-12-01 2023-12-08
```

Roda o pipeline inteiro via `spark-submit` direto, sem depender da UI do
Airflow — só precisa do cluster Spark de pé.

## Arquitetura

Ingestão lê o `incoming/` do dia (Parquet ou CSV, dependendo da origem) →
Bronze (raw + metadados) → Silver (dedup + `DataQualityFramework`, separa
válidos de quarentena) → Gold (duas tabelas: faturamento por estado e
faturamento mensal) → quality gate → notificação.

Diagrama detalhado e as decisões de design (por que 3 scripts separados, por
que a imagem customizada do Airflow, etc.) estão em
[`docs/arquitetura.md`](docs/arquitetura.md).

## Qualidade de dados

`quality/checks.py` implementa um framework próprio (sem Great Expectations
nem Soda, conforme pedido na especificação) com:

- **Completude** — campos obrigatórios (`order_id`, `customer_id`,
  `product_id`, `total_amount`, `order_date`)
- **Unicidade** — `order_id` não pode se repetir
- **Validade de domínio** — quantidade positiva, valor não-negativo, status
  dentro do conjunto esperado

Quem falha em alguma regra vai pra `data/datalake/quarentena/vendas/` com o
motivo registrado em `quarantine_reasons`, em vez de simplesmente ser
descartado.

## Estrutura do repositório

```
.
├── docker-compose.yml
├── docker/airflow.Dockerfile
├── dags/pipeline.py
├── spark_jobs/
│   ├── ingestao.py        # Bronze
│   ├── transformacao.py   # Silver + quality gate
│   └── agregacao.py       # Gold
├── quality/checks.py      # DataQualityFramework
├── scripts/
│   ├── airflow_init.sh
│   └── rodar_pipeline_local.sh   # plano B
├── data/raw/               # dados de entrada (incoming + dimensões)
└── docs/arquitetura.md
```

## Troubleshooting rápido

| Sintoma | Causa provável | Solução |
|---|---|---|
| `SparkSubmitOperator` falha com "spark-submit: command not found" | Imagem do Airflow não foi rebuildada | `docker compose build airflow-init airflow-webserver airflow-scheduler` |
| DAG não aparece na UI | Scheduler ainda não fez o parse, ou erro de import | `docker compose logs airflow-scheduler` / `docker exec shopbrasil-airflow-scheduler airflow dags list-import-errors` |
| `FileNotFoundError` no job Bronze | Data errada ou pasta `incoming/<data>` não existe | Confira `data/raw/incoming/` — só existe de 2023-12-01 a 2023-12-08 |
| Quality gate falha sempre | Relatório da Silver não foi gerado | Rode a Silver antes (`silver_transformacao` precisa ter sucesso antes do `quality_gate`) |
| `Mkdirs failed to create file:...` num dos jobs Spark | Permissão — o driver (dentro do container do Airflow) e os executors (no cluster Spark) rodam com uids diferentes, e um tranca o outro fora do diretório que criou primeiro | Já corrigido via `spark.hadoop.fs.permissions.umask-mode=000` nas SparkSessions. Se acontecer mesmo assim, apague `data/datalake/` e rode de novo |
| Uma execução aparece sozinha pra "hoje" assim que a DAG é ativada, e fica presa no sensor por um tempo | Comportamento padrão do Airflow: `catchup=False` + `schedule` cron dispara a última janela automaticamente ao despausar | Normal, não quebra nada — o sensor falha suave (`soft_fail`) em ~1 min e as tasks seguintes são puladas. Ignore essa execução e foque na que você disparou manualmente |
