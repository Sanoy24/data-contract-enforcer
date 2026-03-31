# Data Contract Enforcer

The Data Contract Enforcer turns every arrow in your inter-system data flow diagram into a formal, machine-checked promise.

## Prerequisites

```bash
# Python 3.12+ required
python --version

# Install dependencies via uv
uv sync
```

## Quick Start — Full Pipeline

### Step 1: Generate sample data (if needed)

```bash
uv run python scripts/generate_sample_data.py
```

Expected output: JSONL files in `outputs/week1/` through `outputs/week5/` and `outputs/traces/`, each with 50+ records.

### Step 2: Generate contracts

```bash
# Week 3 — Document Refinery Extractions
uv run python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

# Week 5 — Event Records
uv run python contracts/generator.py \
  --source outputs/week5/events.jsonl \
  --contract-id week5-event-records \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

Expected output:
- `generated_contracts/week3_extractions.yaml` (≥8 clauses, Bitol-compatible)
- `generated_contracts/week3_extractions_dbt.yml` (dbt schema.yml)
- `generated_contracts/week5_events.yaml` (≥6 clauses)
- `generated_contracts/week5_events_dbt.yml`
- Schema snapshots in `schema_snapshots/`

### Step 3: Run validation (clean data)

```bash
uv run python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/baseline_run.json
```

Expected output: `validation_reports/baseline_run.json` — all structural and statistical checks **PASS**.

### Step 4: Inject a violation and re-validate

```bash
uv run python scripts/create_violation.py

uv run python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/violated_run.json
```

Expected output: `validation_reports/violated_run.json` — confidence range check **FAIL** with `CRITICAL` severity.

### Step 5: Attribute the violation

```bash
uv run python contracts/attributor.py \
  --violation validation_reports/violated_run.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --contract generated_contracts/week3_extractions.yaml \
  --output violation_log/violations.jsonl
```

### Step 6: Schema evolution analysis

```bash
uv run python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution.json
```

### Step 7: AI contract extensions

```bash
uv run python contracts/ai_extensions.py \
  --mode all \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json
```

### Step 8: Generate the Enforcer Report

```bash
uv run python contracts/report_generator.py
```

Expected output: `enforcer_report/report_data.json` with `data_health_score` between 0 and 100.

## Repository Structure

```
├── contracts/                  # Core enforcement scripts
│   ├── generator.py            # ContractGenerator
│   ├── runner.py               # ValidationRunner
│   ├── attributor.py           # ViolationAttributor
│   ├── schema_analyzer.py      # SchemaEvolutionAnalyzer
│   ├── ai_extensions.py        # AI Contract Extensions
│   └── report_generator.py     # Enforcer Report generator
├── generated_contracts/        # Auto-generated YAML contracts + dbt
├── validation_reports/         # Structured validation report JSON
├── violation_log/              # Violation records JSONL
├── schema_snapshots/           # Timestamped schema snapshots
├── enforcer_report/            # Stakeholder report data
├── outputs/                    # Input data (weeks 1–5 + traces)
├── scripts/                    # Utility scripts
├── DOMAIN_NOTES.md             # Phase 0 domain reconnaissance
└── README.md                   # This file
```
