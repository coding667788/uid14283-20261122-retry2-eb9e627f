"""Offline simulator for an isolated maintenance-gateway receive endpoint.

The package deliberately contains no networking code.  Every command reads a
local JSON case document, optionally a local SQLite file, and writes one
deterministic JSON document to stdout.
"""

__all__ = ["cli", "engine", "storage", "validation", "jsonio", "errors"]
__version__ = "1.0.0"
