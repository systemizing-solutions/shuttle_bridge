"""fsspec-restic-lite: lightweight backup tool using fsspec."""

from data_shuttle_bridge.file_backup.runtime import (
    init_repo,
    run_backup,
    list_snapshots,
    run_restore,
)
from data_shuttle_bridge.file_backup.repo.repository import (
    Repository,
    Snapshot,
    FileEntry,
)
from data_shuttle_bridge.file_backup.pipeline.chunking import (
    ChunkingStrategy,
    FixedSizeChunker,
)

__all__ = [
    "init_repo",
    "run_backup",
    "list_snapshots",
    "run_restore",
    "Repository",
    "Snapshot",
    "FileEntry",
    "ChunkingStrategy",
    "FixedSizeChunker",
]
