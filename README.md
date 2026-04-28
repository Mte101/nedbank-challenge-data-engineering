# Nedbank DE Challenge — Stage 1 Submission

A data engineering pipeline that ingests raw banking data, applies transformations, and produces a Gold layer of Delta tables ready for analytical querying.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- On Windows: [WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install) with Docker Desktop WSL integration enabled

---

## 1. Build the Base Image

The pipeline extends a provided base image. Build it once before building the submission image.

```bash
docker build -t nedbank-de-challenge/base:1.0 -f ../infrastructure/Dockerfile.base ../infrastructure
```

---

## 2. Build the Submission Image

Run from this directory (`stage1/starter_kit/`):

```bash
docker build -t my-submission:test .
```

---

## 3. Prepare Test Data

Create the required directory structure and copy your input files:

```bash
sudo mkdir -p /tmp/test-data/input /tmp/test-data/config /tmp/test-data/output
sudo chmod 777 /tmp/test-data /tmp/test-data/input /tmp/test-data/config /tmp/test-data/output

cp config/pipeline_config.yaml    /tmp/test-data/config/
cp <your-data>/accounts.csv       /tmp/test-data/input/
cp <your-data>/customers.csv      /tmp/test-data/input/
cp <your-data>/transactions.jsonl /tmp/test-data/input/
```

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
  my-submission:test

echo "Exit code: $?"
```

A successful run exits with code `0` and writes output to `/tmp/test-data/output/`.

---

## 5. Verify Outputs

```bash
ls /tmp/test-data/output/bronze/
ls /tmp/test-data/output/silver/
ls /tmp/test-data/output/gold/
```

Expected Gold layer tables:
- `gold/fact_transactions/`
- `gold/dim_accounts/`
- `gold/dim_customers/`

Each table is written in Delta Lake format and contains a `_delta_log/` directory.

---

## 6. Run the Test Harness

From `stage1/infrastructure/`:

```bash
bash run_tests.sh \
  --stage 1 \
  --data-dir /tmp/test-data \
  --image my-submission:test
```

All 5 checks must pass before submitting.

---

## Pipeline Architecture

| Stage | Module | Input | Output |
|---|---|---|---|
| Bronze | `pipeline/ingest.py` | Raw CSV / JSONL from `/data/input/` | Delta tables in `output/bronze/` |
| Silver | `pipeline/transform.py` | Bronze Delta tables | Cleaned Delta tables in `output/silver/` |
| Gold | `pipeline/provision.py` | Silver Delta tables | Dimensional model in `output/gold/` |

Entry point: `pipeline/run_all.py` — runs ingest → transform → provision in sequence.

---

## Repository Layout

```
stage1/starter_kit/
├── Dockerfile
├── requirements.txt
├── pipeline/
│   ├── run_all.py
│   ├── ingest.py
│   ├── transform.py
│   ├── provision.py
│   └── utils.py
├── config/
│   └── pipeline_config.yaml
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
