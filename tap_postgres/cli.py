"""Command-line interface (SPEC §1.1)."""

from __future__ import annotations

import argparse
from dataclasses import KW_ONLY, dataclass
from importlib import metadata
from pathlib import Path

import singer
from singer import utils

from tap_postgres import config as cfg
from tap_postgres import db, discovery, sync

LOGGER = singer.get_logger()


@dataclass(init=False)
class CommandLineInput:
    _: KW_ONLY
    config: Path
    state: Path | None
    catalog: Path | None
    discover: bool


def parse_args() -> CommandLineInput:
    parser = argparse.ArgumentParser(
        prog="tap-postgres",
        description="A singer tap for the PostgreSQL RDBMS",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {metadata.version('tap-postgres')}",
    )
    parser.add_argument(
        "-c",
        "--config",
        required=True,
        help="JSON configuration file",
        type=Path,
    )
    parser.add_argument(
        "-s",
        "--state",
        help="JSON state file from a previous run",
        type=Path,
    )
    parser.add_argument(
        "--catalog",
        help="Catalog file with stream selection metadata",
        type=Path,
    )
    parser.add_argument(
        "-d",
        "--discover",
        action="store_true",
        help="Run discovery mode",
    )
    return parser.parse_args(namespace=CommandLineInput())


def main_impl() -> None:
    args = parse_args()
    config = utils.load_json(args.config)
    cfg.validate_config(config)

    if args.discover:
        connection = db.open_connection(config)
        try:
            discovery.do_discovery(
                connection,
                itersize=cfg.itersize(config),
                filter_schemas=cfg.filter_schemas(config),
            )
        finally:
            connection.close()
    elif args.catalog:
        catalog = utils.load_json(args.catalog)
        state = utils.load_json(args.state) if args.state else {}
        # The state *path* is retained: log-based sync periodically re-reads it
        # to learn what the downstream target has committed (SPEC §6.3.7).
        sync.do_sync(config, catalog, state, args.state)
    else:
        LOGGER.info("No properties were selected: pass --discover or --catalog")


def main() -> None:
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise
