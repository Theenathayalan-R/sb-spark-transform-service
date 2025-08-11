from __future__ import annotations

import argparse
import logging

from .pipeline import TransformationPipeline
from .logging_utils import setup_logging


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PySpark Transformation Service Framework")
    parser.add_argument("--config", "-c", help="Path to unified configuration YAML file")
    parser.add_argument("--pipeline-config", help="Path to pipeline configuration YAML file")
    parser.add_argument("--connection-config", help="Path to connection (spark/starburst) YAML file")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without executing")
    parser.add_argument("--restart", action="store_true", help="Restart from last successful step if supported")
    # New options
    parser.add_argument("--job-id", help="Optional job id to use for logging and tracking")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging level",
    )
    args = parser.parse_args(argv)

    # Configure logging level early
    setup_logging(args.log_level)

    if not args.config and not (args.pipeline_config and args.connection_config):
        parser.error("Provide --config or both --pipeline-config and --connection-config")

    pipeline = TransformationPipeline(
        config_path=args.config,
        job_id=args.job_id,
        restart=args.restart,
        pipeline_config=args.pipeline_config,
        connection_config=args.connection_config,
    )
    if args.dry_run:
        logging.info("Dry run mode - validating configuration")
        order = pipeline._get_execution_order()
        logging.info("Configuration valid. Found %d execution batches", len(order))
        logging.info("Execution batches: %s", [[s.name for s in batch] for batch in order])
        return 0
    pipeline.execute()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
