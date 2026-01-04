"""CLI entry point for shuttle_bridge."""

from .cli import main


def cli():
    """Entry point for console script."""
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
