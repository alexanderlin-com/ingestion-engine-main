import os
import sys
import json
import datetime
from dotenv import load_dotenv

from utils.parsing import parse_file
from utils.chunking import chunk_parsed_docs
from utils.pinecone_ops import upsert_chunks
from utils.hashing import make_doc_id

load_dotenv()

LEDGER_PATH = "ledger.jsonl"

def append_to_ledger(entry: dict):
    """Append a JSON line to the ledger file."""
    with open(LEDGER_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

def process_file(file_path: str):
    try:
        # Load file bytes (needed for hashing)
        with open(file_path, "rb") as f:
            file_bytes = f.read()

        # Parse -> Chunk
        parsed = parse_file(file_path)
        chunks = chunk_parsed_docs(parsed)

        # Upsert
        upsert_chunks(file_path, file_bytes, chunks)

        # Ledger entry
        entry = {
            "doc_id": make_doc_id(file_path, file_bytes),
            "source_path": file_path,
            "chunk_count": len(chunks),
            "parser": parsed[0]["parser"] if parsed else None,
            "ingested_at": datetime.datetime.utcnow().isoformat(),
            "status": "success",
            "error": None,
        }
        append_to_ledger(entry)
        print(f"[OK] {file_path} -> {len(chunks)} chunks ingested.")

    except Exception as e:
        entry = {
            "doc_id": None,
            "source_path": file_path,
            "chunk_count": 0,
            "parser": None,
            "ingested_at": datetime.datetime.utcnow().isoformat(),
            "status": "failed",
            "error": str(e),
        }
        append_to_ledger(entry)
        print(f"[FAIL] {file_path}: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python ingestion.py <documents_folder>")
        sys.exit(1)

    folder = sys.argv[1]
    if not os.path.isdir(folder):
        print(f"Error: '{folder}' is not a directory")
        sys.exit(1)

    files = [os.path.join(folder, f) for f in os.listdir(folder)]
    print(f"Found {len(files)} files in {folder}")

    for file_path in files:
        process_file(file_path)

if __name__ == "__main__":
    main()

