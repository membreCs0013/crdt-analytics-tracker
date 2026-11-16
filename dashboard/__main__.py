"""dashboard/__main__.py — entry point for `python -m dashboard`.

Usage:
    python -m dashboard [--port 8080] [--compose docker-compose.yml]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m dashboard",
        description="CRDT Analytics Tracker — Live Dashboard",
    )
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on (default: 8080)")
    parser.add_argument(
        "--compose",
        default=str(PROJECT_ROOT / "docker-compose.yml"),
        help="Path to docker-compose.yml (default: <project root>/docker-compose.yml)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    compose_path = Path(args.compose).resolve()

    from dashboard.server import create_app
    app = create_app(compose_path=compose_path)

    # Use waitress for a production-quality server (already in requirements)
    try:
        from waitress import serve
        print(f"Dashboard running at http://127.0.0.1:{args.port}")
        print(f"Compose file: {compose_path}")
        print("Press Ctrl+C to stop.")
        serve(app, host="127.0.0.1", port=args.port, threads=8)
    except ImportError:
        # Fallback to Flask dev server (not for production)
        app.run(host="127.0.0.1", port=args.port, debug=False)

    return 0


if __name__ == "__main__":
    sys.exit(main())
