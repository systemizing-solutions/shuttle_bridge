"""CLI commands for fsspec-restic-lite backup tool."""

import argparse

from data_shuttle_bridge.file_backup.runtime import (
    init_repo,
    run_backup,
    list_snapshots,
    run_restore,
)


def cmd_backup_init(args: argparse.Namespace) -> int:
    """Initialize a backup repository."""
    try:
        init_repo(args.repo_url)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


def cmd_backup_backup(args: argparse.Namespace) -> int:
    """Backup one or more sources."""
    try:
        run_backup(args.repo_url, args.sources)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


def cmd_backup_snapshots(args: argparse.Namespace) -> int:
    """List snapshots."""
    try:
        list_snapshots(args.repo_url)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


def cmd_backup_restore(args: argparse.Namespace) -> int:
    """Restore a snapshot."""
    try:
        run_restore(args.repo_url, args.dest, args.snapshot_id)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


def add_backup_commands(subparsers) -> None:
    """Add backup subcommands to the main parser."""
    # Main backup parser
    p_backup = subparsers.add_parser("backup", help="Backup commands")
    backup_sub = p_backup.add_subparsers(dest="backup_cmd", required=True)

    # backup init
    p_init = backup_sub.add_parser("init", help="Initialize a backup repository")
    p_init.add_argument(
        "repo_url",
        help="fsspec URL for the repository (e.g., file:///path, s3://bucket/prefix, memory://repo)",
    )
    p_init.set_defaults(func=cmd_backup_init)

    # backup backup
    p_backup_cmd = backup_sub.add_parser("backup", help="Create a backup")
    p_backup_cmd.add_argument(
        "repo_url",
        help="fsspec URL for the repository",
    )
    p_backup_cmd.add_argument(
        "sources",
        nargs="+",
        help="One or more local file/directory paths to backup",
    )
    p_backup_cmd.set_defaults(func=cmd_backup_backup)

    # backup snapshots
    p_snapshots = backup_sub.add_parser("snapshots", help="List snapshots")
    p_snapshots.add_argument(
        "repo_url",
        help="fsspec URL for the repository",
    )
    p_snapshots.set_defaults(func=cmd_backup_snapshots)

    # backup restore
    p_restore = backup_sub.add_parser("restore", help="Restore a snapshot")
    p_restore.add_argument(
        "repo_url",
        help="fsspec URL for the repository",
    )
    p_restore.add_argument(
        "dest",
        help="Destination directory for restored files",
    )
    p_restore.add_argument(
        "--snapshot-id",
        "-s",
        dest="snapshot_id",
        default=None,
        help="Snapshot ID to restore (defaults to latest)",
    )
    p_restore.set_defaults(func=cmd_backup_restore)
