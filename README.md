# Data Contract Enforcer

The Data Contract Enforcer turns every arrow in your inter-system data flow diagram into a formal, machine-checked promise. When a promise is broken — by a schema change, a type drift, a statistical shift — the Enforcer catches it, traces it to the commit that caused it, and produces a blast radius report showing every downstream system affected.

## Prerequisites

```bash
# Python 3.12+ required
python --version

# Install dependencies
uv sync
# or
pip install -r requirements.txt
```

## LLM Configuration (Optional)

The system uses LLMs for column annotation (generator) and embedding drift detection (AI extensions). Without configuration it falls back to heuristics — no API key is required to run the full pipeline.

```bash
cp .env.example .env
# Edit .env with your preferred provider
```

Supported providers: **OpenAI**, **Anthropic**, **Gemini**, **Ollama** (local), **OpenRouter**, or any OpenAI-compatible API. See `.env.example` for all options.

Quick examples:

```bash
# Gemini
export LLM_PROVIDER=gemini
export GEMINI_API_KEY=your-key

# Local Ollama (no key needed)
export LLM_PROVIDER=ollama
export LLM_MODEL=llama3.2

# OpenRouter
export LLM_PROVIDER=openrouter
export OPENROUTER_API_KEY=your-key
```

## Quick Start — Full Pipeline

### Step 1: Generate sample data (if needed)

```bash
uv run python scripts/generate_sample_data.py
```

Expected: JSONL files in `outputs/week1/` through `outputs/week5/` and `outputs/traces/`, each with 50+ records.

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

Expected:
- `generated_contracts/week3_extractions.yaml` (>=8 clauses, Bitol-compatible)
- `generated_contracts/week3_extractions_dbt.yml` (dbt schema.yml)
- `generated_contracts/week5_events.yaml` (>=6 clauses)
- `generated_contracts/week5_events_dbt.yml`
- Schema snapshots in `schema_snapshots/`

### Step 3: Run validation on clean data (establishes baseline)

```bash
uv run python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --mode AUDIT \
  --output validation_reports/baseline_run.json
```

Expected: `validation_reports/baseline_run.json` — all checks PASS, baselines written to `schema_snapshots/baselines.json`.

### Step 4: Inject a violation and re-validate

```bash
uv run python scripts/create_violation.py

uv run python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --mode ENFORCE \
  --output validation_reports/violated_run.json
```

Expected: `validation_reports/violated_run.json` — confidence range check FAIL (CRITICAL) + statistical drift FAIL (HIGH). Pipeline BLOCKED. Violations written to `violation_log/violations.jsonl`.

### Step 5: Attribute the violation

```bash
uv run python contracts/attributor.py \
  --violation validation_reports/violated_run.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output violation_log/violations.jsonl
```

Expected: Blame chain with ranked candidates (commit hash, author, confidence score) and blast radius from registry subscribers.

### Step 6: Schema evolution analysis

```bash
uv run python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution_week3.json
```

Expected: Schema diff showing BREAKING change (statistical shift in confidence mean), migration checklist, and rollback plan.

### Step 7: AI contract extensions

```bash
uv run python contracts/ai_extensions.py \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json
```

Expected: Embedding drift score, prompt schema validation result, LLM output violation rate. Run twice — first run sets baseline, second run compares.

### Step 8: Generate the Enforcer Report

```bash
uv run python contracts/report_generator.py \
  --output enforcer_report/report_data.json
```

Expected: `enforcer_report/report_data.json` with `data_health_score` between 0 and 100, all 5 report sections populated.

Verify: `cat enforcer_report/report_data.json | python -m json.tool` — confirm health score, violations, schema changes, AI risk, and recommendations are present.

## Repository Structure

```
data-contract-enforcer/
├── contracts/                  # Core enforcement modules
│   ├── generator.py            # ContractGenerator — profiles data, generates Bitol YAML
│   ├── runner.py               # ValidationRunner — executes contract checks (AUDIT/WARN/ENFORCE)
│   ├── attributor.py           # ViolationAttributor — blame chains via lineage + git
│   ├── schema_analyzer.py      # SchemaEvolutionAnalyzer — diffs snapshots, classifies changes
│   ├── ai_extensions.py        # AI Contract Extensions — embedding drift, prompt schema, output rate
│   ├── report_generator.py     # ReportGenerator — auto-generates Enforcer Report
│   └── llm_client.py           # Unified LLM client (OpenAI/Anthropic/Gemini/Ollama/OpenRouter)
├── contract_registry/          # Subscription declarations
│   └── subscriptions.yaml      # Who depends on which contract and which fields
├── generated_contracts/        # Auto-generated YAML contracts + dbt schema.yml
├── validation_reports/         # Structured validation report JSON
├── violation_log/              # Violation records JSONL (blame chains + blast radius)
├── schema_snapshots/           # Timestamped schema snapshots + baselines
├── enforcer_report/            # Auto-generated stakeholder report
├── outputs/                    # Input data (weeks 1-5 + LangSmith traces)
├── scripts/                    # Utility scripts (sample data, violation injection)
├── DOMAIN_NOTES.md             # Phase 0 domain reconnaissance
├── .env.example                # LLM provider configuration template
├── requirements.txt            # pip dependencies
├── pyproject.toml              # Project config (uv)
└── README.md
```

## Enforcement Modes

| Mode | Behavior |
|------|----------|
| `AUDIT` (default) | Log all violations, never block pipeline |
| `WARN` | Block on CRITICAL violations only |
| `ENFORCE` | Block on CRITICAL or HIGH violations |

```bash
uv run python contracts/runner.py --contract <contract.yaml> --data <data.jsonl> --mode ENFORCE
```
