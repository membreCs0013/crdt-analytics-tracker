"""Entry point for running a node process with Waitress WSGI server."""

import logging
import signal
import sys
from typing import Any

import waitress

from node.app import create_app
from node.config import Config


def setup_logging(node_id: str) -> None:
    """Configure structured logging containing the node identifier."""
    log_format = f"%(asctime)s [%(levelname)s] [{node_id}] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    """Main execution function."""
    config = Config.from_env_and_args(sys.argv[1:])
    setup_logging(config.node_id)
    logger = logging.getLogger("node.main")

    logger.info(
        "Starting node %s on %s:%d (data_dir=%s, metrics=%s)",
        config.node_id,
        config.host,
        config.port,
        config.data_dir,
        config.metrics,
    )

    app = create_app(config)
    ctx = app.extensions["node"]

    # Register graceful shutdown signals
    shutdown_called = False

    def shutdown(sig: int = 0, frame: Any = None) -> None:
        nonlocal shutdown_called
        if shutdown_called:
            return
        shutdown_called = True
        logger.info("Graceful shutdown triggered for node %s", config.node_id)
        ctx.stop()
        sys.exit(0)

    try:
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
    except (ValueError, AttributeError):
        pass  # On certain non-main threads or restricted platforms

    # Start gossip and background persistence threads
    ctx.start()

    try:
        waitress.serve(
            app,
            host=config.host,
            port=config.port,
            threads=8,
            _quiet=True,
        )
    finally:
        shutdown()


if __name__ == "__main__":
    main()
