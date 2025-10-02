# utils/pinecone_ops.py
import os
import time
from typing import List, Dict
from dotenv import load_dotenv
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI
from utils.hashing import make_doc_id, make_chunk_id, sha256_digest

load_dotenv()

# --- Setup ---
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index_name = os.getenv("PINECONE_INDEX_NAME")

# Create index if missing
if index_name not in pc.list_indexes().names():
    print(f"Index '{index_name}' not found. Creating...")
    pc.create_index(
        name=index_name,
        dimension=3072,  # matches text-embedding-3-large
        metric="cosine",
        spec=ServerlessSpec(cloud="aws", region="us-east-1"),
    )
    while not pc.describe_index(index_name).status["ready"]:
        time.sleep(1)

index = pc.Index(index_name)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- Functions ---
def embed_texts(texts: List[str]) -> List[List[float]]:
    """Embed a list of texts using OpenAI embeddings."""
    response = client.embeddings.create(
        model="text-embedding-3-large",
        input=texts
    )
    return [item.embedding for item in response.data]

def upsert_chunks(file_path: str, file_bytes: bytes, chunks: List[Dict]):
    """
    Given chunks from chunking.py, embed and upsert them into Pinecone.
    Each chunk gets a deterministic chunk_id.
    """
    doc_id = make_doc_id(file_path, file_bytes)

    texts = [chunk["text"] for chunk in chunks]
    embeddings = embed_texts(texts)

    vectors = []
    for chunk, emb in zip(chunks, embeddings):
        content_hash = sha256_digest(chunk["text"].encode("utf-8"))
        chunk_id = f"{doc_id}-{chunk['chunk_index']}"

        vectors.append({
            "id": chunk_id,
            "values": emb,
            "metadata": {
                "doc_id": doc_id,
                "text": chunk["text"],
                "page_start": chunk["page_start"],
                "page_end": chunk["page_end"],
                "section_path": " > ".join(chunk["section_path"]),
                "parser": chunk["parser"],
                "ocr": chunk["ocr"],
                "chunk_index": chunk["chunk_index"],
                "content_hash": content_hash,
            }
        })

    index.upsert(vectors=vectors)
    print(f"Upserted {len(vectors)} chunks for doc {doc_id}")
