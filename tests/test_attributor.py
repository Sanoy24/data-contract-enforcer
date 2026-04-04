"""Tests for contracts/attributor.py — blast radius, lineage, blame chain."""

from datetime import datetime, timezone, timedelta

import pytest

from contracts.attributor import (
    compute_transitive_depth,
    find_upstream_files,
    registry_blast_radius,
    score_candidates,
)


class TestRegistryBlastRadius:
    def test_match_confidence_field(self, sample_registry):
        affected = registry_blast_radius(
            "week3-document-refinery-extractions", "fact_confidence", sample_registry
        )
        assert len(affected) >= 1
        subscriber_ids = [a["subscriber_id"] for a in affected]
        assert "week4-cartographer" in subscriber_ids

    def test_no_match_unrelated_field(self, sample_registry):
        affected = registry_blast_radius(
            "week3-document-refinery-extractions", "totally_unrelated_field", sample_registry
        )
        assert len(affected) == 0

    def test_no_match_wrong_contract(self, sample_registry):
        affected = registry_blast_radius(
            "nonexistent-contract", "fact_confidence", sample_registry
        )
        assert len(affected) == 0


class TestFindUpstreamFiles:
    def test_finds_week3_nodes(self, sample_lineage_graph):
        files = find_upstream_files("week3-document-refinery-extractions", sample_lineage_graph)
        assert len(files) > 0
        paths = [f[0] for f in files]
        assert any("week3" in p for p in paths)

    def test_returns_hop_counts(self, sample_lineage_graph):
        files = find_upstream_files("week3-document-refinery-extractions", sample_lineage_graph)
        for path, hops in files:
            assert isinstance(hops, int)
            assert hops >= 0


class TestComputeTransitiveDepth:
    def test_finds_transitive_nodes(self, sample_lineage_graph):
        result = compute_transitive_depth(["week3"], sample_lineage_graph)
        assert "direct" in result
        assert "transitive" in result
        assert "max_depth" in result
        assert isinstance(result["max_depth"], int)

    def test_empty_subscribers(self, sample_lineage_graph):
        result = compute_transitive_depth([], sample_lineage_graph)
        assert result["max_depth"] == 0


class TestScoreCandidates:
    def test_ranking_by_recency(self):
        now = datetime.now(timezone.utc)
        commits = [
            {"commit_hash": "aaa", "author": "a@x.com",
             "commit_timestamp": (now - timedelta(days=1)).isoformat(),
             "commit_message": "recent", "file_path": "f.py", "lineage_hops": 0},
            {"commit_hash": "bbb", "author": "b@x.com",
             "commit_timestamp": (now - timedelta(days=5)).isoformat(),
             "commit_message": "older", "file_path": "f.py", "lineage_hops": 0},
        ]
        ranked = score_candidates(commits)
        assert ranked[0]["commit_hash"] == "aaa"
        assert ranked[0]["confidence_score"] > ranked[1]["confidence_score"]

    def test_max_five_candidates(self):
        now = datetime.now(timezone.utc)
        commits = [
            {"commit_hash": f"hash{i}", "author": "a@x.com",
             "commit_timestamp": (now - timedelta(days=i)).isoformat(),
             "commit_message": f"msg{i}", "file_path": "f.py", "lineage_hops": 0}
            for i in range(10)
        ]
        ranked = score_candidates(commits)
        assert len(ranked) <= 5

    def test_confidence_formula(self):
        now = datetime.now(timezone.utc)
        commits = [
            {"commit_hash": "aaa", "author": "a@x.com",
             "commit_timestamp": now.isoformat(),
             "commit_message": "just now", "file_path": "f.py", "lineage_hops": 1},
        ]
        ranked = score_candidates(commits)
        # confidence = 1.0 - (0 days * 0.1) - (1 hop * 0.2) = 0.8
        assert ranked[0]["confidence_score"] == pytest.approx(0.8, abs=0.05)

    def test_deduplicates_by_hash(self):
        now = datetime.now(timezone.utc)
        commits = [
            {"commit_hash": "same", "author": "a@x.com",
             "commit_timestamp": now.isoformat(),
             "commit_message": "msg", "file_path": "f1.py", "lineage_hops": 0},
            {"commit_hash": "same", "author": "a@x.com",
             "commit_timestamp": now.isoformat(),
             "commit_message": "msg", "file_path": "f2.py", "lineage_hops": 0},
        ]
        ranked = score_candidates(commits)
        assert len(ranked) == 1
