# utils/chunking.py
from typing import List, Dict
import tiktoken

# load OpenAI tokenizer (cl100k_base works for most models like gpt-3.5/4)
enc = tiktoken.get_encoding("cl100k_base")

def count_tokens(text: str) -> int:
    """Return the number of tokens in a string."""
    return len(enc.encode(text))

def chunk_text(
    text: str,
    max_tokens: int = 500,
    overlap: int = 50
) -> List[str]:
    """
    Split text into chunks by tokens with overlap.
    """
    tokens = enc.encode(text)
    chunks = []
    start = 0

    while start < len(tokens):
        end = min(start + max_tokens, len(tokens))
        chunk = tokens[start:end]
        chunks.append(enc.decode(chunk))
        start += max_tokens - overlap

    return chunks

def chunk_parsed_docs(parsed_docs: List[Dict], max_tokens: int = 500, overlap: int = 50):
    """
    Given parsed docs (from parsing.py), break them into token-based chunks.
    Each output chunk carries metadata from the original section.
    """
    results = []
    for section in parsed_docs:
        text = section["text"]
        pieces = chunk_text(text, max_tokens=max_tokens, overlap=overlap)

        for i, piece in enumerate(pieces):
            results.append({
                "text": piece,
                "page_start": section["page_start"],
                "page_end": section["page_end"],
                "section_path": section["section_path"],
                "parser": section["parser"],
                "ocr": section["ocr"],
                "chunk_index": i
            })
    return results
