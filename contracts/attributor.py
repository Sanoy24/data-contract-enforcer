"""
ViolationAttributor — Traces violations to upstream commits via lineage + git blame.

Usage:
    uv run python contracts/attributor.py \
        --violation validation_reports/violated_run.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --registry contract_registry/subscriptions.yaml \
        --output violation_log/violations.jsonl
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml

try:
    from git import Repo
except ImportError:
    Repo = None


# ---------------------------------------------------------------------------
# Registry blast radius (primary source)
# ---------------------------------------------------------------------------


def load_registry(registry_path: str) -> list[dict]:
    """Load subscription entries from the contract registry YAML."""
    with open(registry_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("subscriptions", [])


def registry_blast_radius(
    contract_id: str, failing_field: str, registry: list[dict]
) -> list[dict]:
    """Find all subscribers affected by a failing field via registry lookup.

    This is the PRIMARY source for blast radius — not the lineage graph.
    """
    affected = []
    for sub in registry:
        if sub.get("contract_id") != contract_id:
            continue
        for bf in sub.get("breaking_fields", []):
            bf_field = bf.get("field", "") if isinstance(bf, dict) else bf
            bf_reason = bf.get("reason", "") if isinstance(bf, dict) else ""
            # Match field names flexibly:
            # Registry uses dotted paths like "extracted_facts.confidence"
            # Runner uses flattened names like "fact_confidence" (prefix stripped)
            # We match if the leaf parts overlap
            bf_leaf = bf_field.split(".")[-1] if "." in bf_field else bf_field
            fail_leaf = failing_field.split("_")[-1] if "_" in failing_field else failing_field
            bf_flat = bf_field.replace(".", "_")
            if (
                bf_field == failing_field
                or bf_flat == failing_field
                or bf_leaf == fail_leaf  # "confidence" == "confidence"
                or failing_field.endswith(bf_leaf)  # "fact_confidence" ends with "confidence"
                or bf_field in failing_field
                or failing_field in bf_flat
            ):
                affected.append(
                    {
                        "subscriber_id": sub["subscriber_id"],
                        "subscriber_team": sub.get("subscriber_team", "unknown"),
                        "contact": sub.get("contact", "unknown"),
                        "validation_mode": sub.get("validation_mode", "AUDIT"),
                        "reason": bf_reason,
                    }
                )
                break
    return affected


# ---------------------------------------------------------------------------
# Lineage graph traversal (enrichment)
# ---------------------------------------------------------------------------


def load_lineage_graph(lineage_path: str) -> dict:
    """Load the latest lineage snapshot from JSONL."""
    with open(lineage_path, encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if not lines:
        return {"nodes": [], "edges": []}
    return json.loads(lines[-1])


def find_upstream_files(
    contract_id: str, graph: dict, max_depth: int = 3
) -> list[tuple[str, int]]:
    """BFS upstream from producer nodes to find source files.

    Returns list of (file_path, hop_count) tuples.
    """
    # Extract week tag from contract_id (e.g. "week3" from "week3-document-refinery-extractions")
    parts = contract_id.split("-")
    week_tag = parts[0] if parts else ""

    # Find producer nodes matching the week tag
    producer_nodes = set()
    for node in graph.get("nodes", []):
        nid = node.get("node_id", "")
        meta_path = node.get("metadata", {}).get("path", "")
        if week_tag in nid or week_tag in meta_path:
            producer_nodes.add(nid)

    if not producer_nodes:
        # Fallback: use all nodes
        producer_nodes = {n["node_id"] for n in graph.get("nodes", [])}

    # BFS upstream: follow edges where target is in our frontier
    visited = set()
    frontier = set(producer_nodes)
    results = []

    for depth in range(1, max_depth + 1):
        next_frontier = set()
        for edge in graph.get("edges", []):
            if edge["target"] in frontier and edge["source"] not in visited:
                source = edge["source"]
                visited.add(source)
                next_frontier.add(source)
                # Extract file path from node_id (e.g. "file::src/week3/extractor.py")
                file_path = source.split("::")[-1] if "::" in source else source
                results.append((file_path, depth))
        frontier = next_frontier

    # Also include the producer nodes themselves at depth 0
    for nid in producer_nodes:
        file_path = nid.split("::")[-1] if "::" in nid else nid
        results.append((file_path, 0))

    return results


def compute_transitive_depth(
    subscriber_ids: list[str], graph: dict, max_depth: int = 2
) -> dict:
    """Compute transitive contamination depth from affected subscribers."""
    # Find nodes matching subscriber IDs
    sub_nodes = set()
    for node in graph.get("nodes", []):
        nid = node.get("node_id", "")
        for sid in subscriber_ids:
            sid_tag = sid.split("-")[0] if "-" in sid else sid
            if sid_tag in nid:
                sub_nodes.add(nid)

    visited = set()
    frontier = set(sub_nodes)
    depth_map = {}

    for depth in range(1, max_depth + 1):
        next_frontier = set()
        for edge in graph.get("edges", []):
            if edge["source"] in frontier and edge["target"] not in visited:
                target = edge["target"]
                if target not in depth_map:
                    depth_map[target] = depth
                visited.add(target)
                next_frontier.add(target)
        frontier = next_frontier

    return {
        "direct": [n for n, d in depth_map.items() if d == 1],
        "transitive": [n for n, d in depth_map.items() if d > 1],
        "max_depth": max(depth_map.values()) if depth_map else 0,
    }


# ---------------------------------------------------------------------------
# Git blame for cause attribution
# ---------------------------------------------------------------------------


def get_git_commits(
    file_paths: list[str], repo_root: str = ".", days: int = 14
) -> list[dict]:
    """Get recent commits for given file paths using gitpython."""
    commits = []

    if Repo is None:
        return commits

    try:
        repo = Repo(repo_root)
    except Exception:
        return commits

    # Try each file path; fall back to all recent commits if paths don't exist
    found_any = False
    for fpath, hops in file_paths:
        try:
            for c in repo.iter_commits(paths=fpath, max_count=5):
                commits.append(
                    {
                        "commit_hash": c.hexsha,
                        "author": c.author.email if c.author else "unknown",
                        "commit_timestamp": datetime.fromtimestamp(
                            c.committed_date, tz=timezone.utc
                        ).isoformat(),
                        "commit_message": c.message.strip().split("\n")[0],
                        "file_path": fpath,
                        "lineage_hops": hops,
                    }
                )
                found_any = True
        except Exception:
            continue

    # If no commits found for lineage paths, get recent repo commits
    if not found_any:
        try:
            for c in repo.iter_commits(max_count=10):
                commits.append(
                    {
                        "commit_hash": c.hexsha,
                        "author": c.author.email if c.author else "unknown",
                        "commit_timestamp": datetime.fromtimestamp(
                            c.committed_date, tz=timezone.utc
                        ).isoformat(),
                        "commit_message": c.message.strip().split("\n")[0],
                        "file_path": "repository-root",
                        "lineage_hops": 1,
                    }
                )
        except Exception:
            pass

    return commits


def score_candidates(commits: list[dict]) -> list[dict]:
    """Score and rank blame candidates.

    Formula: confidence = 1.0 - (days_since_commit * 0.1) - (lineage_hops * 0.2)
    Clamped to [0.05, 1.0]. Max 5 candidates returned.
    """
    now = datetime.now(timezone.utc)
    scored = []

    # Deduplicate by commit hash
    seen = set()
    for c in commits:
        if c["commit_hash"] in seen:
            continue
        seen.add(c["commit_hash"])

        try:
            ct = datetime.fromisoformat(c["commit_timestamp"])
        except (ValueError, TypeError):
            ct = now
        days = abs((now - ct).total_seconds()) / 86400
        hops = c.get("lineage_hops", 1)
        score = max(0.05, min(1.0, round(1.0 - (days * 0.1) - (hops * 0.2), 3)))
        scored.append({**c, "confidence_score": score})

    scored.sort(key=lambda x: x["confidence_score"], reverse=True)
    # Assign rank to top 5
    result = []
    for rank, s in enumerate(scored[:5], 1):
        result.append(
            {
                "rank": rank,
                "file_path": s["file_path"],
                "commit_hash": s["commit_hash"],
                "author": s["author"],
                "commit_timestamp": s["commit_timestamp"],
                "commit_message": s["commit_message"],
                "confidence_score": s["confidence_score"],
            }
        )
    return result


# ---------------------------------------------------------------------------
# Violation log writer
# ---------------------------------------------------------------------------


def write_violation_entry(
    check_result: dict,
    blame_chain: list[dict],
    blast_radius: dict,
    output_path: str,
) -> dict:
    """Write an enriched violation entry to the violation log."""
    entry = {
        "violation_id": str(uuid.uuid4()),
        "check_id": check_result.get("check_id", "unknown"),
        "contract_id": check_result.get("contract_id", "unknown"),
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "blame_chain": blame_chain,
        "blast_radius": blast_radius,
        "severity": check_result.get("severity", "UNKNOWN"),
        "message": check_result.get("message", ""),
        "records_failing": check_result.get("records_failing", 0),
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")

    return entry


# ---------------------------------------------------------------------------
# Main attribution pipeline
# ---------------------------------------------------------------------------


def run_attribution(
    violation_report_path: str,
    lineage_path: str,
    registry_path: str,
    repo_root: str = ".",
    output_path: str = "violation_log/violations.jsonl",
) -> list[dict]:
    """Full attribution pipeline: registry -> lineage -> git blame -> write."""

    # Load validation report
    with open(violation_report_path, encoding="utf-8") as f:
        report = json.load(f)

    contract_id = report.get("contract_id", "unknown")

    # Load registry and lineage
    registry = load_registry(registry_path)
    graph = load_lineage_graph(lineage_path)

    # Find all FAIL results
    fails = [r for r in report.get("results", []) if r["status"] == "FAIL"]
    if not fails:
        print("No failures found in report. Nothing to attribute.")
        return []

    # Get upstream files via lineage
    upstream_files = find_upstream_files(contract_id, graph)

    # Get git commits for those files
    git_commits = get_git_commits(upstream_files, repo_root)

    entries = []
    for fail in fails:
        col_name = fail.get("column_name", "")

        # Step 1: Registry blast radius (primary)
        affected_subs = registry_blast_radius(contract_id, col_name, registry)

        # Step 2: Lineage transitive depth (enrichment)
        sub_ids = [s["subscriber_id"] for s in affected_subs]
        transitive = compute_transitive_depth(sub_ids, graph)

        blast = {
            "source": "registry",
            "direct_subscribers": affected_subs,
            "affected_nodes": [
                n.split("::")[-1] if "::" in n else n
                for n in transitive.get("direct", [])
            ],
            "transitive_nodes": [
                n.split("::")[-1] if "::" in n else n
                for n in transitive.get("transitive", [])
            ],
            "contamination_depth": transitive.get("max_depth", 0),
            "estimated_records": fail.get("records_failing", 0),
            "note": "direct_subscribers from registry; transitive_nodes from lineage graph enrichment",
        }

        # Step 3: Git blame chain
        blame_chain = score_candidates(git_commits)

        # Step 4: Write violation entry
        check_with_contract = {**fail, "contract_id": contract_id}
        entry = write_violation_entry(check_with_contract, blame_chain, blast, output_path)
        entries.append(entry)

        print(f"\nAttributed: {fail['check_id']}")
        print(f"  Blame chain: {len(blame_chain)} candidates")
        if blame_chain:
            top = blame_chain[0]
            print(
                f"  Top candidate: {top['commit_hash'][:8]} by {top['author']} "
                f"(confidence: {top['confidence_score']})"
            )
        print(f"  Blast radius: {len(affected_subs)} direct subscribers")
        print(f"  Contamination depth: {transitive.get('max_depth', 0)}")

    return entries


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="ViolationAttributor — trace violations to upstream commits"
    )
    parser.add_argument(
        "--violation",
        required=True,
        help="Path to validation report JSON with FAIL results",
    )
    parser.add_argument(
        "--lineage",
        default="outputs/week4/lineage_snapshots.jsonl",
        help="Path to Week 4 lineage snapshots JSONL",
    )
    parser.add_argument(
        "--registry",
        default="contract_registry/subscriptions.yaml",
        help="Path to contract registry subscriptions YAML",
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Root directory of the git repository for blame",
    )
    parser.add_argument(
        "--output",
        default="violation_log/violations.jsonl",
        help="Output path for violation log JSONL",
    )

    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("ViolationAttributor")
    print(f"{'='*60}")
    print(f"  Violation report: {args.violation}")
    print(f"  Lineage: {args.lineage}")
    print(f"  Registry: {args.registry}")
    print()

    entries = run_attribution(
        args.violation,
        args.lineage,
        args.registry,
        args.repo_root,
        args.output,
    )

    print(f"\nAttribution complete: {len(entries)} violations attributed")
    print(f"Violation log: {args.output}")


if __name__ == "__main__":
    main()
