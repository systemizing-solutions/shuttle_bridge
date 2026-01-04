"""Runtime functions for backup operations."""

import os
import socket
import stat
from pathlib import Path
from typing import Optional
import uuid

from data_shuttle_bridge.file_backup.repo.repository import (
    Repository,
    Snapshot,
    FileEntry,
)


def init_repo(url: str) -> None:
    """
    Initialize a new backup repository.

    Args:
        url: fsspec URL for the repository.

    Raises:
        RuntimeError: If repository already exists.
    """
    repo = Repository(url)
    if repo.exists():
        raise RuntimeError(f"Repository already exists at {url}")
    repo.init()
    print(f"Initialized repository at {url}")


def run_backup(
    repo_url: str,
    sources: list[str],
    snapshot_id: Optional[str] = None,
) -> str:
    """
    Perform a backup of one or more local sources.

    Args:
        repo_url: fsspec URL for the repository.
        sources: List of local file/directory paths to backup.
        snapshot_id: Optional snapshot ID. If None, generates a new one.

    Returns:
        The snapshot ID.

    Raises:
        RuntimeError: If repository doesn't exist or sources are invalid.
    """
    repo = Repository(repo_url)
    repo._ensure_initialized()

    if not snapshot_id:
        snapshot_id = uuid.uuid4().hex

    # Validate sources
    for source in sources:
        if not os.path.exists(source):
            raise RuntimeError(f"Source does not exist: {source}")

    hostname = socket.gethostname()
    files: list[FileEntry] = []
    file_index: dict[str, list[str]] = {}

    # Process each source
    for source_root in sources:
        if os.path.isfile(source_root):
            # Single file - use the file itself as the source root for relative path
            files_to_process = [(source_root, os.path.dirname(source_root))]
        else:
            # Directory: walk and collect all files
            files_to_process = []
            for dirpath, dirnames, filenames in os.walk(source_root):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    # Store source_root so we can compute relative paths
                    files_to_process.append((filepath, source_root))

        # Process files
        for filepath, root in files_to_process:
            try:
                stat_info = os.stat(filepath)
                file_size = stat_info.st_size
                mtime_ns = int(stat_info.st_mtime_ns)
                mode = stat_info.st_mode

                blobs = []
                blob_hashes = []

                # Chunk and upload
                with open(filepath, "rb") as f:
                    for chunk_data, chunk_hash in repo.chunker.chunk(f):
                        repo.put_blob(chunk_data, chunk_hash)
                        blobs.append({"hash": chunk_hash, "size": len(chunk_data)})
                        blob_hashes.append(chunk_hash)

                # Compute relative path from source root
                # Store both absolute path (for index) and relative path (for restore)
                rel_path = os.path.relpath(filepath, root)

                # Create file entry - store the relative path for restoration
                entry = FileEntry(
                    path=rel_path,
                    size=file_size,
                    mtime_ns=mtime_ns,
                    mode=mode,
                    blobs=blobs,
                )
                files.append(entry)
                file_index[filepath] = blob_hashes

            except Exception as e:
                print(f"Warning: Skipped {filepath}: {e}")

    # Create and write snapshot
    import time

    snapshot = Snapshot(
        snapshot_id=snapshot_id,
        created_at=time.time(),
        hostname=hostname,
        sources=sources,
        files=files,
    )
    repo.write_snapshot(snapshot)
    repo.write_index(snapshot_id, file_index)

    print(f"Snapshot {snapshot_id[:8]} created with {len(files)} files")
    return snapshot_id


def list_snapshots(repo_url: str) -> None:
    """
    List all snapshots in a repository.

    Args:
        repo_url: fsspec URL for the repository.
    """
    repo = Repository(repo_url)
    repo._ensure_initialized()

    snapshots = repo.list_snapshots()
    if not snapshots:
        print("No snapshots found.")
        return

    print(f"{'Snapshot ID':<12} {'Created At':<26} {'Hostname':<15} {'Files':<6}")
    print("-" * 60)
    for snap in snapshots:
        from datetime import datetime

        created_str = datetime.utcfromtimestamp(snap.created_at).isoformat()
        print(
            f"{snap.snapshot_id[:8]:<12} {created_str:<26} "
            f"{snap.hostname:<15} {len(snap.files):<6}"
        )


def run_restore(
    repo_url: str,
    dest: str,
    snapshot_id: Optional[str] = None,
) -> None:
    """
    Restore a snapshot to a destination directory.

    Args:
        repo_url: fsspec URL for the repository.
        dest: Destination directory for restored files.
        snapshot_id: Snapshot ID to restore. If None, restores the latest.

    Raises:
        RuntimeError: If repository doesn't exist or snapshot not found.
    """
    repo = Repository(repo_url)
    repo._ensure_initialized()

    # Get snapshot
    if snapshot_id:
        snapshot = repo.get_snapshot_by_id(snapshot_id)
        if not snapshot:
            raise RuntimeError(f"Snapshot not found: {snapshot_id}")
    else:
        snapshots = repo.list_snapshots()
        if not snapshots:
            raise RuntimeError("No snapshots found")
        snapshot = snapshots[0]  # Latest (already sorted desc)

    # Ensure dest exists
    os.makedirs(dest, exist_ok=True)

    # Restore files
    for file_entry in snapshot.files:
        # Use relative path stored in the entry
        file_path = os.path.join(dest, file_entry.path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Reconstruct file from blobs
        with open(file_path, "wb") as f:
            for blob_info in file_entry.blobs:
                blob_data = repo.get_blob(blob_info["hash"])
                f.write(blob_data)

        # Restore metadata (best-effort)
        try:
            os.chmod(file_path, stat.S_IMODE(file_entry.mode))
        except Exception:
            pass

        try:
            # mtime_ns is nanoseconds since epoch
            os.utime(file_path, ns=(file_entry.mtime_ns, file_entry.mtime_ns))
        except Exception:
            pass

    print(
        f"Restored snapshot {snapshot.snapshot_id[:8]} "
        f"({len(snapshot.files)} files) to {dest}"
    )
