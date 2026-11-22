"""Structured error type shared by validation, protocol and storage layers.

Every failure the simulator reports carries a stable machine code and a
JSON-pointer style ``path`` into the case document (for example
``batches/0/events/2/fingerprint``).  The path is empty when the problem is
not tied to a single document location (for example a checksum mismatch of
the whole database).
"""


class SimError(Exception):
    __slots__ = ("code", "path", "detail")

    def __init__(self, code, path="", detail=""):
        super().__init__(code)
        self.code = code
        self.path = path
        self.detail = detail

    def to_dict(self):
        return {"code": self.code, "path": self.path, "detail": self.detail}

    def __repr__(self):  # pragma: no cover - debugging aid
        return "SimError(%r, %r, %r)" % (self.code, self.path, self.detail)
