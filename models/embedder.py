"""
Financial News Embedding Module — Tier 3 Semantic Deduplication Engine

Uses fastembed (ONNX Runtime) to run all-MiniLM-L6-v2 in <15ms on CPU
without pulling the 2GB PyTorch dependency tree.

Vectors are L2-normalized at output, so cosine similarity = dot product.
"""

import logging
import threading
from typing import List, Optional
import numpy as np

logger = logging.getLogger(__name__)

# Singleton instance — lazily initialized on first use.
# Guarded by a lock: main.py runs RSS/GDELT/SEC workers in parallel threads,
# and an unlocked lazy init would let two threads each load the ONNX model.
_EMBEDDER_INSTANCE: Optional["FinancialEmbedder"] = None
_EMBEDDER_LOCK = threading.Lock()

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384


class FinancialEmbedder:
    """
    Singleton wrapper around fastembed's all-MiniLM-L6-v2 model.

    - embed_text(text) → np.ndarray of shape (384,), L2-normalized.
    - embed_batch(texts) → np.ndarray of shape (N, 384), L2-normalized.

    Thread Safety: the singleton is constructed under a lock. fastembed's ONNX
    session (onnxruntime.InferenceSession.run) is documented thread-safe for
    concurrent inference; tests/test_semantic_embedding.py includes a concurrent
    smoke test that verifies outputs match single-threaded execution.
    """

    def __init__(self):
        from fastembed import TextEmbedding

        logger.info(f"Loading embedding model: {MODEL_NAME} (ONNX Runtime)...")
        self._model = TextEmbedding(model_name=MODEL_NAME)
        logger.info(f"Embedding model loaded. Output dimension: {EMBEDDING_DIM}")

    def embed_text(self, text: str) -> np.ndarray:
        """
        Embeds a single text string into a 384-dim L2-normalized vector.

        Args:
            text: Input text (headline, snippet, etc.)

        Returns:
            np.ndarray of shape (384,), dtype float32, L2-normalized.
        """
        if not text or not text.strip():
            return np.zeros(EMBEDDING_DIM, dtype=np.float32)

        # fastembed returns a generator of numpy arrays
        embeddings = list(self._model.embed([text]))
        vec = embeddings[0].astype(np.float32)

        # L2-normalize (fastembed already normalizes, but we ensure it)
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm

        return vec

    def embed_batch(self, texts: List[str]) -> np.ndarray:
        """
        Embeds a batch of text strings into L2-normalized 384-dim vectors.

        Args:
            texts: List of input text strings.

        Returns:
            np.ndarray of shape (N, 384), dtype float32, L2-normalized rows.
        """
        if not texts:
            return np.empty((0, EMBEDDING_DIM), dtype=np.float32)

        # Replace empty strings with a placeholder to avoid model errors
        cleaned = [t if t and t.strip() else " " for t in texts]
        embeddings = list(self._model.embed(cleaned))
        matrix = np.array(embeddings, dtype=np.float32)

        # L2-normalize each row
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0  # prevent division by zero
        matrix = matrix / norms

        return matrix


def get_embedder() -> FinancialEmbedder:
    """
    Returns the singleton FinancialEmbedder instance.
    Lazily initializes on first call (downloads model weights if needed),
    under a lock with double-checked locking so parallel workers cannot
    each construct the model.
    """
    global _EMBEDDER_INSTANCE
    if _EMBEDDER_INSTANCE is None:
        with _EMBEDDER_LOCK:
            if _EMBEDDER_INSTANCE is None:
                _EMBEDDER_INSTANCE = FinancialEmbedder()
    return _EMBEDDER_INSTANCE
