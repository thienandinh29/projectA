"""
Verification script for DuckDB Vector Embeddings and Semantic Search.
"""

from lakehouse.db import LakehouseManager
from models.embedder import get_embedder

def main():
    lh = LakehouseManager(read_only=True)
    stats = lh.get_stats()
    print("=== Lakehouse Status ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print()

    embedder = get_embedder()
    queries = [
        "Federal Reserve interest rate hike and inflation policy",
        "Stock market rally and corporate earnings growth",
        "Oil and energy price volatility"
    ]

    for q in queries:
        print(f"Query: '{q}'")
        print("-" * 65)
        vec = embedder.embed_text(q).tolist()
        results = lh.query_similar_news(query_embedding=vec, limit=3)
        for i, r in enumerate(results, 1):
            print(f"  {i}. [Cosine: {r['score']:.4f}] {r['title'][:65]}")
            print(f"     Source: {r['source']} | Time: {r['effective_time']}")
        print()

    lh.close()

if __name__ == "__main__":
    main()
