"""
Unified LLM client — reads provider config from environment variables.

Supports: OpenAI, Anthropic, Gemini, Ollama, OpenRouter, or any OpenAI-compatible API.
Falls back to a heuristic (no-op) when no provider is configured.

Environment variables:
    LLM_PROVIDER    — one of: openai, anthropic, gemini, ollama, openrouter  (default: auto-detect)
    LLM_MODEL       — model name override (e.g. "gemma2", "mistral", "claude-3-5-haiku-20241022")
    LLM_API_KEY     — API key (not needed for ollama)
    LLM_BASE_URL    — custom base URL (required for ollama / openrouter, optional for others)

    Provider-specific keys are also read as fallbacks:
        OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY, OPENROUTER_API_KEY

    Embedding-specific overrides:
        EMBEDDING_PROVIDER  — if you want embeddings from a different provider than chat
        EMBEDDING_MODEL     — model name for embeddings (default: provider-dependent)
"""

import hashlib
import os
from pathlib import Path

import numpy as np


# ---------------------------------------------------------------------------
# Config from env
# ---------------------------------------------------------------------------

def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def get_llm_config() -> dict:
    """Resolve which LLM provider + model + key to use from env."""
    provider = _env("LLM_PROVIDER", "").lower()
    model = _env("LLM_MODEL", "")
    api_key = _env("LLM_API_KEY", "")
    base_url = _env("LLM_BASE_URL", "")

    # Auto-detect from available keys if provider not set
    if not provider:
        if _env("ANTHROPIC_API_KEY"):
            provider = "anthropic"
            api_key = api_key or _env("ANTHROPIC_API_KEY")
        elif _env("GEMINI_API_KEY"):
            provider = "gemini"
            api_key = api_key or _env("GEMINI_API_KEY")
        elif _env("OPENROUTER_API_KEY"):
            provider = "openrouter"
            api_key = api_key or _env("OPENROUTER_API_KEY")
        elif _env("OPENAI_API_KEY"):
            provider = "openai"
            api_key = api_key or _env("OPENAI_API_KEY")
        elif _env("OLLAMA_BASE_URL") or base_url:
            provider = "ollama"
        else:
            provider = "none"

    # Defaults per provider
    defaults = {
        "openai":     {"model": "gpt-4o-mini",                "base_url": ""},
        "anthropic":  {"model": "claude-3-5-haiku-20241022",   "base_url": ""},
        "gemini":     {"model": "gemini-2.0-flash",            "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/"},
        "ollama":     {"model": "llama3.2",                    "base_url": _env("OLLAMA_BASE_URL", "http://localhost:11434/v1")},
        "openrouter": {"model": "openai/gpt-4o-mini",         "base_url": "https://openrouter.ai/api/v1"},
        "none":       {"model": "",                            "base_url": ""},
    }

    d = defaults.get(provider, defaults["none"])
    model = model or d["model"]
    base_url = base_url or d["base_url"]

    # Provider-specific key fallbacks
    if not api_key:
        fallback_keys = {
            "openai": "OPENAI_API_KEY",
            "anthropic": "ANTHROPIC_API_KEY",
            "gemini": "GEMINI_API_KEY",
            "openrouter": "OPENROUTER_API_KEY",
        }
        api_key = _env(fallback_keys.get(provider, ""), "")

    return {
        "provider": provider,
        "model": model,
        "api_key": api_key,
        "base_url": base_url,
    }


def get_embedding_config() -> dict:
    """Resolve embedding provider config (can differ from chat LLM)."""
    provider = _env("EMBEDDING_PROVIDER", "").lower()
    model = _env("EMBEDDING_MODEL", "")

    if not provider:
        # Fall back to LLM_PROVIDER
        llm = get_llm_config()
        provider = llm["provider"]
        base_url = llm["base_url"]
        api_key = llm["api_key"]
    else:
        api_key = _env("LLM_API_KEY", "") or _env(f"{provider.upper()}_API_KEY", "")
        base_url = _env("LLM_BASE_URL", "")

    emb_defaults = {
        "openai":     "text-embedding-3-small",
        "gemini":     "text-embedding-004",
        "ollama":     "nomic-embed-text",
        "openrouter": "openai/text-embedding-3-small",
    }
    model = model or emb_defaults.get(provider, "")

    return {
        "provider": provider,
        "model": model,
        "api_key": api_key,
        "base_url": base_url,
    }


# ---------------------------------------------------------------------------
# Chat completion (used by generator.py LLM annotation)
# ---------------------------------------------------------------------------

def chat_completion(prompt: str, max_tokens: int = 300) -> str | None:
    """Send a single-turn chat message. Returns the text response or None on failure."""
    cfg = get_llm_config()
    provider = cfg["provider"]

    if provider == "none":
        return None

    # Anthropic uses its own SDK
    if provider == "anthropic":
        return _chat_anthropic(prompt, cfg, max_tokens)

    # Everything else goes through the OpenAI-compatible interface
    return _chat_openai_compat(prompt, cfg, max_tokens)


def _chat_anthropic(prompt: str, cfg: dict, max_tokens: int) -> str | None:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=cfg["api_key"] or None)
        resp = client.messages.create(
            model=cfg["model"],
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text if resp.content else None
    except Exception:
        return None


def _chat_openai_compat(prompt: str, cfg: dict, max_tokens: int) -> str | None:
    """Works for OpenAI, Gemini, Ollama, OpenRouter — anything with an OpenAI-compatible API."""
    try:
        from openai import OpenAI
        kwargs = {}
        if cfg["api_key"]:
            kwargs["api_key"] = cfg["api_key"]
        if cfg["base_url"]:
            kwargs["base_url"] = cfg["base_url"]
        # Ollama doesn't need an API key but the SDK requires a non-empty string
        if cfg["provider"] == "ollama" and not cfg["api_key"]:
            kwargs["api_key"] = "ollama"

        client = OpenAI(**kwargs)
        resp = client.chat.completions.create(
            model=cfg["model"],
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
        )
        return resp.choices[0].message.content or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Embeddings (used by ai_extensions.py)
# ---------------------------------------------------------------------------

def embed_texts(texts: list[str], n: int = 200) -> tuple[np.ndarray, str]:
    """Embed texts using the configured provider. Returns (vectors, method_name).

    Falls back to deterministic hash-based mock if no provider is available.
    """
    cfg = get_embedding_config()
    sample = texts[:n]

    if cfg["provider"] != "none" and cfg["model"]:
        vecs = _embed_openai_compat(sample, cfg)
        if vecs is not None:
            return vecs, cfg["provider"]

    return _embed_mock(sample), "hash_mock"


def _embed_openai_compat(texts: list[str], cfg: dict, batch_size: int = 100) -> np.ndarray | None:
    """Embeddings via any OpenAI-compatible API, with batching for large inputs.

    Sends texts in chunks of batch_size to avoid API timeouts on large datasets.
    """
    try:
        from openai import OpenAI
        kwargs = {}
        if cfg["api_key"]:
            kwargs["api_key"] = cfg["api_key"]
        if cfg["base_url"]:
            kwargs["base_url"] = cfg["base_url"]
        if cfg["provider"] == "ollama" and not cfg["api_key"]:
            kwargs["api_key"] = "ollama"

        client = OpenAI(**kwargs)
        all_embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            resp = client.embeddings.create(input=batch, model=cfg["model"])
            all_embeddings.extend([e.embedding for e in resp.data])
        return np.array(all_embeddings)
    except Exception:
        return None


def _embed_mock(texts: list[str]) -> np.ndarray:
    """Deterministic hash-based pseudo-embeddings (no API key needed).

    SHA256 -> 32 bytes -> 64 float values normalized to unit vector.
    """
    vectors = []
    for text in texts:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        raw = []
        for b in digest:
            raw.append((b >> 4) / 15.0)
            raw.append((b & 0x0F) / 15.0)
        vec = np.array(raw, dtype=np.float64)
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        vectors.append(vec)
    return np.array(vectors)


def describe_config() -> str:
    """Human-readable summary of active LLM config for logging."""
    cfg = get_llm_config()
    ecfg = get_embedding_config()
    lines = [
        f"Chat: provider={cfg['provider']}, model={cfg['model']}",
        f"Embeddings: provider={ecfg['provider']}, model={ecfg['model']}",
    ]
    if cfg["base_url"]:
        lines[0] += f", base_url={cfg['base_url']}"
    if ecfg["base_url"] and ecfg["base_url"] != cfg["base_url"]:
        lines[1] += f", base_url={ecfg['base_url']}"
    if cfg["provider"] == "none":
        lines.append("(No LLM configured -- using heuristic fallbacks)")
    return "\n".join(lines)
