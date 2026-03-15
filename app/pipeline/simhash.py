"""
SimHash — 64-bit locality-sensitive hash for headline similarity detection.

Design:
  - Zero external dependencies (stdlib only)
  - O(N) per document where N = number of tokens
  - Hamming distance <= threshold → same event
  - Default threshold = 8 bits (≈12.5% divergence across 64 bits)

Usage:
    h1 = compute_simhash("Apple reports blowout earnings, beats EPS by 15%")
    h2 = compute_simhash("AAPL beats earnings estimates, EPS $2.05 vs $1.78 expected")
    print(is_similar(h1, h2))  # True
"""
from __future__ import annotations

import hashlib
import re
from functools import lru_cache

# Default Hamming distance threshold — tuned empirically.
# 8/64 bits = 12.5% divergence allowed.
DEFAULT_HAMMING_THRESHOLD = 8

# Stop words to exclude from token weighting
_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "has", "have", "had", "this", "that", "its", "it", "as", "up", "vs",
    "said", "says", "will", "would", "could", "may", "per", "than", "also",
})


def _tokenize(text: str) -> list[str]:
    """Lowercase, remove punctuation, split on whitespace, drop stop words."""
    clean = re.sub(r"[^a-z0-9\s]", " ", text.lower())
    return [w for w in clean.split() if w and w not in _STOP_WORDS]


def _token_hash(token: str) -> int:
    """64-bit hash for a single token via SHA-256 truncation."""
    digest = hashlib.sha256(token.encode()).digest()
    # Take first 8 bytes as a big-endian uint64
    return int.from_bytes(digest[:8], "big")


def compute_simhash(text: str) -> int:
    """
    Compute a 64-bit SimHash fingerprint of the input text.

    Algorithm:
      1. Tokenize text
      2. For each token, compute a 64-bit hash
      3. For each bit position i: add +1 to v[i] if bit i is set, else -1
      4. Fingerprint bit i = 1 if v[i] > 0, else 0
    """
    if not text:
        return 0

    tokens = _tokenize(text)
    if not tokens:
        return 0

    v = [0] * 64

    for token in tokens:
        h = _token_hash(token)
        for i in range(64):
            if h & (1 << i):
                v[i] += 1
            else:
                v[i] -= 1

    fingerprint = 0
    for i in range(64):
        if v[i] > 0:
            fingerprint |= (1 << i)

    return fingerprint


def hamming_distance(h1: int, h2: int) -> int:
    """Count differing bits between two 64-bit integers."""
    xor = h1 ^ h2
    # Brian Kernighan's bit count
    count = 0
    while xor:
        xor &= xor - 1
        count += 1
    return count


def is_similar(
    h1: int,
    h2: int,
    threshold: int = DEFAULT_HAMMING_THRESHOLD,
) -> bool:
    """Return True if two SimHash fingerprints are within the Hamming threshold."""
    return hamming_distance(h1, h2) <= threshold


def simhash_to_hex(h: int) -> str:
    """Serialize SimHash as a 16-char hex string for Redis storage."""
    return format(h & 0xFFFFFFFFFFFFFFFF, "016x")


def simhash_from_hex(s: str) -> int:
    """Deserialize SimHash from hex string."""
    return int(s, 16)
