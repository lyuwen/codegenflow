# Reasoning Pipeline

[中文](README_zh.md)

A data processing pipeline for verifying, annotating, and managing reasoning responses.

## Setup

1. **Clone the repository** & **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Variables**:
   Create a `.env` file in the root directory:
   ```env
   DB_URL=postgresql://user:password@host:port/database
   ```

## Usage: `run_pipeline.py`

The main entry point is `run_pipeline.py`. It supports several subcommands:

### 1. Verify Responses
Run code verification in a sandbox environment.

**Online Verification**:
```bash
python run_pipeline.py verify --limit 1000
```

**Offline Verification**:
Dump tasks to a file for offline processing, then ingest results.
```bash
# 1. Dump tasks
python run_pipeline.py verify --dump-tasks tasks.jsonl --limit 10000

# 2. (External Process: Run tasks in sandbox and save to results.jsonl)

# 3. Ingest results
python run_pipeline.py verify --ingest-results results.jsonl
```

**Options**:
- `--concurrency`: Number of parallel workers (default: 8).
- `--dryrun`: Verify without updating the DB.
- `--retry-status`: Retry specific statuses (e.g., `failed,error`).
- `--dump-tasks`: Path to export verification tasks.
- `--ingest-results`: Path(s) to import verification results.

### 2. Import Data
Import responses or problems from JSONL files.
```bash
python run_pipeline.py import --pattern "data/*.jsonl"
python run_pipeline.py import-problems --pattern "problems/*.jsonl"
```

### 3. Annotate
Calculate metrics like repetition (CR, LRR), entropy, and reasoning flaws.
```bash
python run_pipeline.py annotate --limit 5000 --concurrency 16
```
**Options**:
- `--redo`: Force re-annotation of already annotated responses.

### 4. Update Status
Bulk update the verification status of responses.
```bash
python run_pipeline.py update-status --status failed --file ids.txt
```

### 5. Generate Prompts
Generate new prompt files for model inference.
```bash
python run_pipeline.py generate --output prompts.jsonl --model deepseek-ai/DeepSeek-R1
```

### 6. Export Data
Export responses to JSONL, filtered by criteria.
```bash
python run_pipeline.py export --output dataset.jsonl --status passed --difficulty hard
```
