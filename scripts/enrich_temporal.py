#!/usr/bin/env python
"""Enrich signals with temporal classification data.

Usage:
    python scripts/enrich_temporal.py /path/to/nodes/directory

The nodes directory should contain the temporal node JSON files,
e.g., losIndex__medicareId__dischargeMonth.json
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from services.signal_hydrator import SignalHydrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main(nodes_directory: Path) -> None:
    """Run temporal enrichment."""
    if not nodes_directory.exists():
        logger.error("Nodes directory does not exist: %s", nodes_directory)
        sys.exit(1)

    logger.info("Starting temporal enrichment from: %s", nodes_directory)

    hydrator = SignalHydrator(nodes_directory=nodes_directory)
    stats = await hydrator.enrich_temporal_data()

    logger.info("Enrichment complete:")
    logger.info("  Signals enriched: %d", stats.get("signals_enriched", 0))
    logger.info("  Signals skipped: %d", stats.get("signals_skipped", 0))
    if "error" in stats:
        logger.error("  Error: %s", stats["error"])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <nodes_directory>")
        sys.exit(1)

    nodes_dir = Path(sys.argv[1])
    asyncio.run(main(nodes_dir))
