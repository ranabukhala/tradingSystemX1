"""
Deduplication enums — shared across pipeline, signal aggregator, and execution.
"""
from __future__ import annotations

from enum import Enum


class DedupTier(str, Enum):
    """Which tier of deduplication caught the item."""
    VENDOR_ID    = "vendor_id"       # Tier 1 — exact vendor-provided ID (Redis SETNX)
    CONTENT_HASH = "content_hash"    # Tier 2 — SHA-256 of title+snippet
    SEMANTIC     = "semantic"        # Tier 3 — SimHash headline similarity cluster
    NONE         = "none"            # Not a duplicate — first representative


class DedupReason(str, Enum):
    """Granular reason a record was classified as non-representative."""
    # Tier 1
    VENDOR_ID_SEEN        = "vendor_id_seen"        # Same vendor ID already processed
    # Tier 2
    CONTENT_HASH_MATCH    = "content_hash_match"    # Byte-identical content from diff vendor
    # Tier 3
    SIMHASH_CLUSTER       = "simhash_cluster"       # Headline semantically similar
    FACT_CONFIRMED        = "fact_confirmed"         # SimHash match + fact fingerprint agreed
    # Signal / Order gates
    SIGNAL_ALREADY_EMITTED = "signal_already_emitted"  # Redis SETNX gate in aggregator
    ORDER_ALREADY_SUBMITTED = "order_already_submitted"  # Redis SETNX gate in execution
    # Not a duplicate
    NEW                   = "new"                   # First representative for this event


class DedupAction(str, Enum):
    """Decision outcome for a given record."""
    PASS             = "pass"             # Representative — continue downstream
    DROP_DUPLICATE   = "drop_duplicate"   # Content/semantic duplicate — emit to news.dropped
    DROP_NON_REP     = "drop_non_rep"     # Non-representative (structural, not content dup)
    ALLOW_OVERRIDE   = "allow_override"   # Force-pass despite duplicate (manual replay)
