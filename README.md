# Nedbank DE Challenge — Stage 3 Submission

A data engineering pipeline that ingests raw banking data, applies transformations, produces a Gold layer of Delta tables, and processes a real-time stream of transaction events into two streaming Gold tables.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- On Windows: [WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install) with Docker Desktop WSL integration enabled

> **Windows users:** all paths below use the WSL convention (`/tmp/test-data`).
> If you are running Docker commands from PowerShell instead of WSL, replace
> `/tmp/test-data` with a Windows path such as `C:\Users\<you>\test-data` and
> adjust `cp` → `Copy-Item`, `mkdir` → `New-Item -ItemType Directory`, etc.

---

## 1. Build the Base Image

The pipeline extends a provided base image. Build it once before building the submission image.

```bash
docker build -t nedbank-de-challenge/base:1.0 -f ../infrastructure/Dockerfile.base ../infrastructure
```

---

## 2. Build the Submission Image

Run from this directory:

```bash
docker build -t my-submission:stage3 .
```

---

## 3. Prepare Test Data

### 3a. Create the directory structure

```bash
sudo mkdir -p \
  /tmp/test-data/input \
  /tmp/test-data/config \
  /tmp/test-data/stream \
  /tmp/test-data/output

sudo chmod 777 \
  /tmp/test-data \
  /tmp/test-data/input \
  /tmp/test-data/config \
  /tmp/test-data/stream \
  /tmp/test-data/output
```

### 3b. Copy batch input files (Stage 2 data)

```bash
cp config/pipeline_config.yaml    /tmp/test-data/config/
cp config/dq_rules.yaml           /tmp/test-data/config/
cp <your-data>/accounts.csv       /tmp/test-data/input/
cp <your-data>/customers.csv      /tmp/test-data/input/
cp <your-data>/transactions.jsonl /tmp/test-data/input/
```

### 3c. Copy stream files (Stage 3 data)

Stream files must be named `stream_YYYYMMDD_HHMMSS_NNNN.jsonl` and placed in the
`stream/` directory. Lexicographic filename order must equal chronological order.

```bash
cp <your-stream-data>/stream_*.jsonl  /tmp/test-data/stream/
```

The pipeline maps host path `/tmp/test-data` → container path `/data`.
The `stream/` subdirectory is therefore visible to the container at `/data/stream/`
with no additional mounts required.

---

## 4. Run the Pipeline

```bash
docker run \
  --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --pids-limit=512 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  --cap-drop=ALL \
  --security-opt no-new-privileges \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v /tmp/test-data:/data \
  my-submission:stage3

echo "Exit code: $?"
```

A successful run exits with code `0`. The streaming loop processes all pre-staged
files in the first poll cycle (~60 s) and then idles until the batch pipeline
finishes, after which the container exits cleanly.

---

## 5. Verify Outputs

### Batch Gold tables (unchanged from Stage 2)

```bash
ls /tmp/test-data/output/gold/fact_transactions/
ls /tmp/test-data/output/gold/dim_accounts/
ls /tmp/test-data/output/gold/dim_customers/
```

### DQ report (Stage 2+)

```bash
cat /tmp/test-data/output/dq_report.json
```

### Streaming Gold tables (Stage 3)

```bash
ls /tmp/test-data/output/stream_gold/current_balances/
ls /tmp/test-data/output/stream_gold/recent_transactions/
```

Both directories must contain a `_delta_log/` subdirectory and at least one
`part-*.parquet` file. Quick record counts with DuckDB (run from WSL or a
machine with DuckDB installed):

```bash
duckdb -c "SELECT COUNT(*) FROM delta_scan('/tmp/test-data/output/stream_gold/current_balances');"
duckdb -c "SELECT COUNT(*) FROM delta_scan('/tmp/test-data/output/stream_gold/recent_transactions');"
duckdb -c "SELECT account_id, current_balance, updated_at FROM delta_scan('/tmp/test-data/output/stream_gold/current_balances') LIMIT 10;"
```

---

## 6. Run the Test Harness

From `stage1/infrastructure/`:

```bash
bash run_tests.sh \
  --stage 2 \
  --data-dir /tmp/test-data \
  --image my-submission:stage3
```

All Stage 1 and Stage 2 checks must still pass on the Stage 3 image.

---

## Pipeline Architecture

| Stage | Module | Input | Output |
|---|---|---|---|
| Bronze | `pipeline/ingest.py` | Raw CSV / JSONL from `/data/input/` | Delta tables in `output/bronze/` |
| Silver | `pipeline/transform.py` | Bronze Delta tables | Cleaned Delta tables in `output/silver/` |
| Gold | `pipeline/provision.py` | Silver Delta tables | Dimensional model in `output/gold/` |
| Stream | `pipeline/stream.py` | JSONL micro-batches from `/data/stream/` | `output/stream_gold/current_balances/` and `output/stream_gold/recent_transactions/` |

Entry point: `pipeline/run_all.py` — starts the streaming polling loop in a background
thread, then runs ingest → transform → provision in sequence. The streaming loop
processes all pre-staged stream files within the first 60-second poll cycle.

---

## Repository Layout

```
nedbank-challenge-data-engineering/
├── Dockerfile
├── requirements.txt
├── adr/
│   └── stage3_adr.md          ← Architecture Decision Record (Stage 3)
├── pipeline/
│   ├── run_all.py             ← Entry point (batch + stream)
│   ├── ingest.py              ← Bronze layer
│   ├── transform.py           ← Silver layer + DQ flagging
│   ├── provision.py           ← Gold layer (batch)
│   ├── stream.py              ← Streaming polling loop (Stage 3)
│   ├── dq_report.py           ← DQ report writer (Stage 2+)
│   └── utils.py               ← Shared utilities
├── config/
│   ├── pipeline_config.yaml   ← Path config (batch + streaming)
│   └── dq_rules.yaml          ← DQ handling rules (Stage 2+)
└── README.md
```

---

## Resource Limits

| Resource | Limit |
|---|---|
| RAM | 2 GB |
| CPU | 2 vCPU |
| Network | None during execution |
| Writable paths | `/data/output/` and `/tmp` (512 MB) |

---

## Stream File Format

Each file in `/data/stream/` must be a valid JSONL file where every line is a
transaction event with the following fields (same schema as `transactions.jsonl`):

```json
{
  "transaction_id": "uuid-string",
  "account_id": "uuid-string",
  "transaction_date": "YYYY-MM-DD",
  "transaction_time": "HH:MM:SS",
  "transaction_type": "DEBIT|CREDIT|FEE|REVERSAL",
  "merchant_category": "string or null",
  "merchant_subcategory": "string or null",
  "amount": 123.45,
  "currency": "ZAR",
  "channel": "POS|APP|ATM|EFT|USSD|INTERNAL or null",
  "location": {"province": "string or null", "city": "string or null", "coordinates": null},
  "metadata": {"device_id": "string or null", "session_id": "string or null", "retry_flag": false}
}
```

Files are discovered in lexicographic order (`stream_20260320_143000_0001.jsonl`
before `stream_20260320_143500_0002.jsonl`). Files that have already been processed
are tracked in memory and skipped on subsequent poll cycles.
t |
|---|---|
| RAM | 2 GB |
| CPU | 2 vCPU |
| Network | None during execution |
| Writable paths | `/data/output/` and `/tmp` (512 MB) |
