# utils/hashing.py
import hashlib

def sha256_digest(content: bytes) -> str:
    """Return SHA256 hex digest for given content."""
    return hashlib.sha256(content).hexdigest()

def make_doc_id(file_path: str, file_bytes: bytes) -> str:
    """
    Generate a stable doc_id from path + content.
    Example: sha256(path + file hash).
    """
    path_bytes = file_path.encode("utf-8")
    file_hash = sha256_digest(file_bytes).encode("utf-8")
    return hashlib.sha256(path_bytes + file_hash).hexdigest()

def make_chunk_id(doc_id: str, page_start: int, page_end: int, span_start: int, span_end: int) -> str:
    """
    Generate deterministic chunk_id for a slice of a document.
    """
    raw = f"{doc_id}:{page_start}-{page_end}:{span_start}-{span_end}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()