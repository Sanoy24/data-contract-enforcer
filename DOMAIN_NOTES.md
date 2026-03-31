# DOMAIN_NOTES.md — Data Contract Enforcer

## Phase 0: Domain Reconnaissance

### Question 1: Backward-Compatible vs. Breaking Schema Changes

A **backward-compatible** change allows existing consumers to continue operating without modification. A **breaking** change forces downstream systems to adapt or fail.

#### Three Backward-Compatible Changes

1. **Adding a nullable `summary` field to Week 3 `extraction_record`:** If the Document Refinery adds `"summary": "optional text"` to its output, the Week 4 Cartographer simply ignores the new field since it only reads `doc_id`, `extracted_facts`, and `entities`. No consumer breaks.

2. **Adding a new enum value `"CONCEPT"` to Week 3 `entity.type`:** The existing set is `{PERSON, ORG, LOCATION, DATE, AMOUNT, OTHER}`. Adding `CONCEPT` is additive — consumers using `if entity.type == "ORG"` still work. The Cartographer would classify `CONCEPT` entities under its generic node handling.

3. **Widening Week 5 `sequence_number` from `int32` to `int64`:** The Event Sourcing Platform's sequence numbers are monotonically increasing integers. Widening the type preserves all existing values and allows for higher counts. No consumer that reads integers is affected.

#### Three Breaking Changes

1. **Renaming Week 3 `confidence` to `confidence_score`:** The Cartographer and any downstream consumer keying on `extracted_facts[*].confidence` would receive `KeyError` exceptions. The field that contracts validate against disappears.

2. **Changing Week 3 `confidence` from float 0.0–1.0 to integer 0–100:** This is the canonical silent corruption example. Structurally, the field is still numeric. But every consumer that treats values > 0.5 as "high confidence" would now treat 51/100 as barely above threshold instead of interpreting it as a percentage. The Cartographer might filter out all facts with `confidence < 0.5`, discarding the entire dataset.

3. **Removing the `metadata.causation_id` field from Week 5 `event_record`:** Any consumer building a causal event chain (e.g., the Week 8 Sentinel) would lose the ability to trace event causation. This silently degrades observability.

---

### Question 2: The Confidence Scale Change — Tracing the Failure

**Scenario:** The Week 3 Document Refinery's `confidence` field changes from `float 0.0–1.0` to `int 0–100`.

**Current distribution in our data:**
```
min=0.55  max=0.98  mean≈0.76
```
All values are within the contracted 0.0–1.0 range.

**Failure propagation to Week 4 Cartographer:**

1. Week 3 outputs `extractions.jsonl` with confidence values like `78`, `92`, `55`.
2. Week 4 Cartographer ingests these records. Its metadata enrichment logic stores `confidence` as node metadata, e.g., `"extraction_confidence": 78`.
3. Any downstream filtering that uses `confidence > 0.7` would now accept all records (since all values > 0.7 on the 0–100 scale).
4. Any averaging or aggregation would produce nonsensical results — e.g., mean confidence of 76.3 instead of 0.763.
5. The lineage graph would contain inflated confidence scores, making all edges appear highly confident when they are not.

**The failure is silent**: no exceptions are thrown, no type errors occur. The data simply means something different.

**Bitol contract clause to catch this:**
```yaml
schema:
  extracted_facts:
    type: array
    items:
      confidence:
        type: number
        minimum: 0.0
        maximum: 1.0
        required: true
        description: >
          Confidence score in 0.0-1.0 float range.
          BREAKING CHANGE if converted to 0-100 integer scale.
          Statistical baseline: mean ≈ 0.76, stddev ≈ 0.12.
          Flag if mean > 1.0 (indicates scale change).
quality:
  type: SodaChecks
  specification:
    checks for extractions:
      - max(confidence) <= 1.0
      - min(confidence) >= 0.0
      - avg(confidence) between 0.3 and 1.0
```

The range check (`max <= 1.0`) catches the scale change immediately. The statistical drift check catches it even if someone changes the scale to 0–1 but with different distribution characteristics.

---

### Question 3: How the Enforcer Uses the Lineage Graph for Blame Chains

When a contract violation is detected (e.g., `Week 3 confidence > 1.0`), the ViolationAttributor traces it through the Week 4 lineage graph:

**Step-by-step graph traversal:**

1. **Identify the failing schema element:** The failing check is `week3.extracted_facts.confidence.range`. The system identifier is `week3`.

2. **Load the lineage graph:** Open the latest snapshot from `outputs/week4/lineage_snapshots.jsonl`. Parse the `nodes[]` and `edges[]` arrays.

3. **Find the producing node:** Search `nodes[]` for nodes whose `node_id` contains `week3` and `type == "FILE"`. This identifies files like `file::src/week3/extractor.py`.

4. **Breadth-first upstream traversal:** From the producing node, follow all incoming edges (edges where the producing node is the `target`). For each upstream node found, check if it could have caused the violation. The traversal stops at:
   - The first external boundary (node type `EXTERNAL`)
   - The filesystem root
   - A maximum depth of 5 hops

5. **Git blame integration:** For each upstream file identified, run:
   ```bash
   git log --follow --since="14 days ago" --format='%H|%ae|%ai|%s' -- src/week3/extractor.py
   ```
   This returns recent commits that modified the file.

6. **Confidence scoring:** Each commit is scored:
   - `base = 1.0 - (days_since_commit × 0.1)`
   - `penalty = lineage_distance × 0.2` (number of hops from the blamed file to the failing field)
   - `score = max(0.0, base - penalty)`
   - More recent commits closer in the lineage graph score higher.

7. **Output blame chain:** The top 1–5 candidates are written to `violation_log/violations.jsonl`, ranked by confidence score. Each entry includes the commit hash, author, timestamp, message, and the blast radius (identifiers of all downstream consumers affected).

---

### Question 4: LangSmith Trace Record Contract

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Records
  version: 1.0.0
  owner: platform-team
  description: >
    LLM trace records exported from LangSmith. One record per LLM/chain/tool run.
    Used for cost analysis, performance monitoring, and AI contract enforcement.
servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl
terms:
  usage: Internal observability data. Do not expose to external consumers.
  limitations: Token counts must be accurate for cost computation.
schema:
  id:
    type: string
    format: uuid
    required: true
    unique: true
    description: Unique run identifier.
  run_type:
    type: string
    required: true
    enum: ["llm", "chain", "tool", "retriever", "embedding"]
    description: Type of LangSmith run.
  start_time:
    type: string
    format: date-time
    required: true
  end_time:
    type: string
    format: date-time
    required: true
    description: Must be >= start_time.
  total_tokens:
    type: integer
    minimum: 0
    required: true
    description: Must equal prompt_tokens + completion_tokens.
  prompt_tokens:
    type: integer
    minimum: 0
    required: true
  completion_tokens:
    type: integer
    minimum: 0
    required: true
  total_cost:
    type: number
    minimum: 0.0
    required: true
    description: Cost in USD. Must be non-negative.
quality:
  type: SodaChecks
  specification:
    checks for traces:
      - missing_count(id) = 0
      - duplicate_count(id) = 0
      # Structural clause
      - values in (run_type) must be in ['llm','chain','tool','retriever','embedding']
      # Statistical clause — end_time must come after start_time
      - expression: "end_time > start_time for all rows"
      # AI-specific clause — token arithmetic must hold
      - expression: "total_tokens = prompt_tokens + completion_tokens"
      # Statistical clause — cost sanity check
      - max(total_cost) < 10.0
lineage:
  upstream:
    - id: langsmith-platform
      description: External LangSmith service where traces originate
  downstream:
    - id: week7-ai-contract-extensions
      description: AI Contract Extensions consume trace data for drift detection
      fields_consumed: [run_type, total_tokens, prompt_tokens, completion_tokens, total_cost]
      breaking_if_changed: [run_type, total_tokens]
```

**Structural clause:** `run_type` must be one of the five enum values. Catches API changes or new run types.

**Statistical clause:** `total_cost < 10.0` — if a single run costs more than $10, something is wrong (prompt injection, infinite loop, or misconfigured model). Also, `total_tokens = prompt_tokens + completion_tokens` as an arithmetic invariant.

**AI-specific clause:** `end_time > start_time` for execution time integrity. A negative duration indicates a clock skew or serialization bug in the LangSmith exporter.

---

### Question 5: Why Contracts Go Stale — Failure Modes and Prevention

**The most common failure mode:** Contracts go stale because the data evolves but the contract does not. This happens when:

1. **Schema changes bypass the contract update process.** A developer changes a field type, adds a column, or modifies an enum, and deploys without updating the contract. The contract still references the old schema and either passes silently (if the check is too lenient) or fires false alarms (if the check is too strict, leading the team to disable it).

2. **Statistical baselines become outdated.** The baseline was captured during a period of low traffic. When traffic increases, the mean and stddev of numeric columns shift. The drift check fires warnings that are dismissed as false positives, training the team to ignore contract violations.

3. **No ownership model.** When nobody is explicitly responsible for maintaining a contract, it decays. Contracts written during initial setup are never revisited.

**How our architecture prevents this:**

- **Auto-generation on every run:** The ContractGenerator runs against current data every time. If the schema changes, the generated contract reflects the change. The SchemaEvolutionAnalyzer diffs the new snapshot against the previous one and classifies the change. This makes contract drift visible.

- **Baseline refresh mechanism:** The ValidationRunner updates baselines after the first successful run and can be configured to refresh baselines periodically. Statistical drift is detected relative to the most recent stable baseline, not a fixed historical value.

- **Lineage-driven blast radius:** Every contract includes `downstream_consumers[]` from the Week 4 lineage graph. When a contract is updated or violated, the blast radius report identifies exactly which systems need attention. This creates a forcing function: contract changes cannot be ignored because they show affected downstream systems.

- **Schema snapshots as audit trail:** Every generator run writes a timestamped snapshot. The evolution history is preserved, making it possible to answer "when did this field change?" by diffing snapshots.
