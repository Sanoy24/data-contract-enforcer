"""Tests for contracts/llm_client.py — config resolution + mock embeddings."""

import os

import numpy as np
import pytest

from contracts.llm_client import (
    _embed_mock,
    describe_config,
    embed_texts,
    get_embedding_config,
    get_llm_config,
)


class TestGetLlmConfig:
    def test_no_env_returns_none_provider(self, monkeypatch):
        # Clear all relevant env vars
        for key in ["LLM_PROVIDER", "LLM_MODEL", "LLM_API_KEY", "LLM_BASE_URL",
                     "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY",
                     "OPENROUTER_API_KEY", "OLLAMA_BASE_URL"]:
            monkeypatch.delenv(key, raising=False)
        cfg = get_llm_config()
        assert cfg["provider"] == "none"

    def test_anthropic_key_detected(self, monkeypatch):
        for key in ["LLM_PROVIDER", "OPENAI_API_KEY", "GEMINI_API_KEY",
                     "OPENROUTER_API_KEY", "OLLAMA_BASE_URL", "LLM_BASE_URL"]:
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
        cfg = get_llm_config()
        assert cfg["provider"] == "anthropic"
        assert cfg["api_key"] == "sk-ant-test"

    def test_explicit_provider_overrides(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        monkeypatch.setenv("LLM_MODEL", "mistral")
        for key in ["OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY"]:
            monkeypatch.delenv(key, raising=False)
        cfg = get_llm_config()
        assert cfg["provider"] == "ollama"
        assert cfg["model"] == "mistral"

    def test_gemini_key_detected(self, monkeypatch):
        for key in ["LLM_PROVIDER", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
                     "OPENROUTER_API_KEY", "OLLAMA_BASE_URL", "LLM_BASE_URL"]:
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("GEMINI_API_KEY", "AI-test-key")
        cfg = get_llm_config()
        assert cfg["provider"] == "gemini"


class TestEmbedMock:
    def test_returns_ndarray(self):
        vecs = _embed_mock(["hello", "world"])
        assert isinstance(vecs, np.ndarray)
        assert vecs.shape == (2, 64)

    def test_deterministic(self):
        v1 = _embed_mock(["test string"])
        v2 = _embed_mock(["test string"])
        np.testing.assert_array_equal(v1, v2)

    def test_different_texts_different_vectors(self):
        v1 = _embed_mock(["text a"])
        v2 = _embed_mock(["text b"])
        assert not np.array_equal(v1, v2)

    def test_unit_normalized(self):
        vecs = _embed_mock(["sample"])
        norm = np.linalg.norm(vecs[0])
        assert norm == pytest.approx(1.0, abs=0.01)


class TestEmbedTexts:
    def test_returns_tuple(self):
        result = embed_texts(["hello"])
        assert isinstance(result, tuple)
        assert len(result) == 2
        vecs, method = result
        assert isinstance(vecs, np.ndarray)
        assert isinstance(method, str)

    def test_samples_n_texts(self):
        texts = [f"text {i}" for i in range(50)]
        vecs, _ = embed_texts(texts, n=10)
        assert vecs.shape[0] == 10


class TestDescribeConfig:
    def test_returns_string(self, monkeypatch):
        for key in ["LLM_PROVIDER", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
                     "GEMINI_API_KEY", "OPENROUTER_API_KEY", "OLLAMA_BASE_URL",
                     "LLM_BASE_URL", "LLM_MODEL", "LLM_API_KEY"]:
            monkeypatch.delenv(key, raising=False)
        desc = describe_config()
        assert isinstance(desc, str)
        assert "provider" in desc.lower()
