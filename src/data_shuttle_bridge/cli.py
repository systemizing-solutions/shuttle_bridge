import os
import sys
import argparse

from data_shuttle_bridge.sql.nodeid import ClientNodeManager
from data_shuttle_bridge.file_backup.cli import add_backup_commands


def cmd_node_init(args: argparse.Namespace) -> int:
    server = args.server or os.environ.get("LOCALFIRST_SERVER")
    if not server:
        print(
            "Provide server via --server or LOCALFIRST_SERVER env var.", file=sys.stderr
        )
        return 2
    mgr = ClientNodeManager()
    node_id = mgr.ensure_node_id(server)
    print(f"device_key={mgr.device_key}")
    print(f"node_id={node_id}")
    print("Saved to ~/.localfirst_sync/config.json")
    return 0


def cmd_node_show(args: argparse.Namespace) -> int:
    mgr = ClientNodeManager()
    print(f"device_key={mgr.device_key}")
    print(f"node_id={mgr.node_id}")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="localfirst-sync", description="Local-first sync tooling"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # Node commands
    p_init = sub.add_parser("node", help="Node management commands")
    sub_node = p_init.add_subparsers(dest="node_cmd", required=True)

    p_node_init = sub_node.add_parser(
        "init", help="Lease or reuse a unique node_id from the server"
    )
    p_node_init.add_argument(
        "--server", help="Server base URL (e.g., http://127.0.0.1:5001)"
    )
    p_node_init.set_defaults(func=cmd_node_init)

    p_node_show = sub_node.add_parser("show", help="Show local device_key and node_id")
    p_node_show.set_defaults(func=cmd_node_show)

    # Backup commands
    add_backup_commands(sub)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
