#!/usr/bin/env python3
"""CLI entrypoint — v1.6.0 (final)"""
from __future__ import annotations
import sys
from pathlib import Path
import click

from harness import S3Harness


@click.command()
@click.option("--config", "config_file", type=click.Path(exists=True, dir_okay=False, path_type=Path), default=Path("config.toml"))
@click.option("--provider", "provider_override", type=str, default=None, help="Provider id from [[providers]] in config.toml")
@click.option("--profile", type=str, default=None, help="AWS shared credentials profile (optional)")
@click.option("--quick", is_flag=True, default=False, help="Reserved for quick mode (dataset/iterations reduced)")
def main(config_file: Path, provider_override: str | None, profile: str | None, quick: bool) -> None:
    h = S3Harness(config_file=str(config_file), provider_override=provider_override, profile=profile)
    sys.exit(h.run())


if __name__ == "__main__":
    main()
