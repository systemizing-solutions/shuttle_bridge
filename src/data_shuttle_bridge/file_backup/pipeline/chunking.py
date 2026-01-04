"""Chunking strategies for fsspec-restic-lite."""

from abc import ABC, abstractmethod
from typing import BinaryIO, Iterator
import hashlib


class ChunkingStrategy(ABC):
    """Base class for chunking strategies."""

    @abstractmethod
    def chunk(self, file_obj: BinaryIO) -> Iterator[tuple[bytes, str]]:
        """
        Chunk a file and yield (data, sha256_hash) tuples.

        Args:
            file_obj: Open file object in binary mode.

        Yields:
            Tuples of (chunk_bytes, sha256_hash_hex).
        """
        pass


class FixedSizeChunker(ChunkingStrategy):
    """Fixed-size chunking strategy."""

    def __init__(self, chunk_size: int = 4 * 1024 * 1024):
        """
        Initialize with a chunk size.

        Args:
            chunk_size: Size of each chunk in bytes. Defaults to 4 MiB.
        """
        self.chunk_size = chunk_size

    def chunk(self, file_obj: BinaryIO) -> Iterator[tuple[bytes, str]]:
        """
        Chunk a file into fixed-size pieces.

        Args:
            file_obj: Open file object in binary mode.

        Yields:
            Tuples of (chunk_bytes, sha256_hash_hex).
        """
        while True:
            data = file_obj.read(self.chunk_size)
            if not data:
                break
            hash_hex = hashlib.sha256(data).hexdigest()
            yield data, hash_hex
