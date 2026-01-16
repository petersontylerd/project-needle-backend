"""Signal generator service for parsing Project Needle node results.

This service parses node result JSONL files from Project Needle's insight graph
and extracts signals based on anomaly detection results.
"""

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from pydantic import ValidationError

from src.db.models import (
    SignalDomain,
)
from src.schemas.signal import (
    NodeEntityResult,
    NodeResults,
    NodeStatisticalMethod,
    NodeStatistics,
    SignalCreate,
    TemporalTimeline,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Mapping Constants
# =============================================================================

# Maps metric IDs to quality domains
METRIC_TO_DOMAIN: dict[str, SignalDomain] = {
    # Efficiency metrics
    "losIndex": SignalDomain.EFFICIENCY,
    "averageLos": SignalDomain.EFFICIENCY,
    "throughput": SignalDomain.EFFICIENCY,
    "edThroughput": SignalDomain.EFFICIENCY,
    "orThroughput": SignalDomain.EFFICIENCY,
    "bedTurnover": SignalDomain.EFFICIENCY,
    # Safety metrics
    "clabsiRate": SignalDomain.SAFETY,
    "vaeRate": SignalDomain.SAFETY,
    "fallRate": SignalDomain.SAFETY,
    "infectionRate": SignalDomain.SAFETY,
    "ssiRate": SignalDomain.SAFETY,
    "cautiRate": SignalDomain.SAFETY,
    "pressureInjuryRate": SignalDomain.SAFETY,
    # Effectiveness metrics
    "readmissionRate": SignalDomain.EFFECTIVENESS,
    "mortalityRate": SignalDomain.EFFECTIVENESS,
    "patientSatisfaction": SignalDomain.EFFECTIVENESS,
    "clinicalOutcome": SignalDomain.EFFECTIVENESS,
}

# Default domain for unknown metrics
DEFAULT_DOMAIN = SignalDomain.EFFICIENCY

# Anomaly levels that should generate signals (9-tier system, exclude "normal" for less noise)
SIGNAL_ANOMALY_LEVELS = frozenset(
    {
        "extremely_high",
        "extremely_low",
        "very_high",
        "very_low",
        "moderately_high",
        "moderately_low",
        "slightly_high",
        "slightly_low",
    }
)


class SignalGeneratorError(Exception):
    """Base exception for signal generator errors.

    Attributes:
        message: Error description.
        path: Optional path to the file that caused the error.

    Example:
        >>> raise SignalGeneratorError("Invalid JSON format", path=Path("file.json"))
    """

    def __init__(self, message: str, path: Path | None = None) -> None:
        """Initialize the error with a message and optional path.

        Args:
            message: Error description.
            path: Path to the file that caused the error.
        """
        self.message = message
        self.path = path
        super().__init__(f"{message}" + (f" (file: {path})" if path else ""))


def _load_node_jsonl(path: Path) -> dict[str, object]:
    """Load a node JSONL file and return data in NodeResults-compatible format.

    The JSONL format uses:
    - Line 1: Node metadata header with type="node_metadata"
    - Lines 2-N: Entity results (one per line)

    Args:
        path: Path to the node JSONL file.

    Returns:
        dict: Data compatible with NodeResults.model_validate()

    Raises:
        ValueError: If the file is empty or has invalid format.
        json.JSONDecodeError: If JSON parsing fails.
    """
    content = path.read_text(encoding="utf-8")
    lines = content.splitlines()

    if not lines:
        raise ValueError(f"Empty node file: {path}")

    # Parse header (line 1)
    header = json.loads(lines[0])
    if header.get("type") != "node_metadata":
        raise ValueError(f"Invalid JSONL format: missing metadata header in {path}")

    # Parse entity results (lines 2-N)
    entity_results = [json.loads(line) for line in lines[1:] if line.strip()]

    # Construct NodeResults-compatible dict
    return {
        "canonical_node_id": header.get("canonical_node_id"),
        "canonical_child_node_ids": header.get("canonical_child_node_ids", []),
        "canonical_parent_node_ids": header.get("canonical_parent_node_ids", []),
        "dataset_path": header.get("dataset_path"),
        "entity_results": entity_results,
    }


class SignalGenerator:
    """Service for parsing Project Needle node results into signals.

    This class parses node result JSONL files and extracts signals based on
    anomaly detection results. It handles domain classification,
    entity field extraction, and temporal classification via trends_to edges.

    Attributes:
        include_normal: Whether to include "normal" anomaly signals.
        nodes_directory: Optional directory containing node result files for
            loading temporal nodes via trends_to edges.

    Example:
        >>> generator = SignalGenerator(nodes_directory=Path("results/nodes"))
        >>> signals = generator.parse_node_results(Path("results/nodes/losIndex.jsonl"))
        >>> for signal in signals:
        ...     print(f"{signal.facility}: {signal.domain}")
    """

    def __init__(
        self,
        include_normal: bool = False,
        nodes_directory: Path | None = None,
    ) -> None:
        """Initialize the signal generator.

        Args:
            include_normal: Whether to include signals with "normal" anomaly level.
                Defaults to False to reduce noise.
            nodes_directory: Optional directory containing node result files.
                When provided, enables loading of temporal nodes via trends_to
                edges for signal classification.
        """
        self.include_normal = include_normal
        self.nodes_directory = nodes_directory
        self._temporal_cache: dict[str, NodeResults | None] = {}

    def _load_temporal_node(self, temporal_node_id: str) -> NodeResults | None:
        """Load temporal node data from file.

        Attempts to load the temporal node JSONL file from the nodes directory.
        Uses caching to avoid reloading the same node multiple times.

        Args:
            temporal_node_id: The canonical ID of the temporal node to load.

        Returns:
            NodeResults: Parsed temporal node data, or None if unavailable.

        Example:
            >>> generator = SignalGenerator(nodes_directory=Path("results/nodes"))
            >>> temporal_data = generator._load_temporal_node(
            ...     "losIndex__medicareId__dischargeMonth"
            ... )
        """
        # Check cache first
        if temporal_node_id in self._temporal_cache:
            return self._temporal_cache[temporal_node_id]

        # Cannot load without nodes directory
        if self.nodes_directory is None:
            self._temporal_cache[temporal_node_id] = None
            return None

        # Construct path to temporal node file
        temporal_path = self.nodes_directory / f"{temporal_node_id}.jsonl"

        if not temporal_path.exists():
            logger.debug(
                "Temporal node file not found: %s",
                temporal_path,
            )
            self._temporal_cache[temporal_node_id] = None
            return None

        try:
            data = _load_node_jsonl(temporal_path)
            temporal_node = NodeResults.model_validate(data)
            self._temporal_cache[temporal_node_id] = temporal_node
            logger.debug(
                "Loaded temporal node: %s with %d entity results",
                temporal_node_id,
                len(temporal_node.entity_results),
            )
            return temporal_node
        except (json.JSONDecodeError, ValidationError, ValueError, OSError) as e:
            logger.warning(
                "Failed to load temporal node %s: %s",
                temporal_node_id,
                e,
            )
            self._temporal_cache[temporal_node_id] = None
            return None

    def _get_temporal_z_scores(
        self,
        temporal_node: NodeResults,
        entity_key: tuple[str, str, str | None],
    ) -> list[float] | None:
        """Extract monthly z-scores for an entity from temporal node data.

        Matches the entity by (facility, service_line, sub_service_line) tuple
        and extracts the z-score timeline from the temporal node's statistical
        methods.

        Args:
            temporal_node: The parsed temporal NodeResults.
            entity_key: Tuple of (facility, service_line, sub_service_line)
                to match against temporal entity results.

        Returns:
            list[float]: List of monthly z-scores in chronological order,
                or None if no matching entity found or no z-scores available.

        Example:
            >>> z_scores = generator._get_temporal_z_scores(
            ...     temporal_node,
            ...     ("AFP658", "Cardiology", None)
            ... )
            >>> print(z_scores)
            [-1.2, -1.1, -1.3, -1.0, ...]
        """
        target_facility, target_service_line, target_sub_service_line = entity_key

        for entity_result in temporal_node.entity_results:
            # Extract entity fields for matching
            facility, service_line, sub_service_line = self._extract_entity_fields(entity_result)

            # Match entity and check for temporal metric
            if (
                facility == target_facility
                and service_line == target_service_line
                and sub_service_line == target_sub_service_line
                and entity_result.metric
                and len(entity_result.metric) > 0
            ):
                metric = entity_result.metric[0]
                if isinstance(metric.values, TemporalTimeline):
                    # Get z-scores from statistical methods if available
                    for stat_method in entity_result.statistical_methods:
                        if "simple_zscore" in stat_method.statistical_method:
                            # Extract z-scores from each period's anomaly
                            # The timeline values are the metric values;
                            # we need the z-scores which are computed per period
                            # For temporal nodes, we use latest_simple_zscore
                            # as a proxy or compute from the values
                            timeline_values = metric.get_timeline_values()
                            if timeline_values and stat_method.statistics.peer_mean:
                                # Compute z-scores from timeline values
                                peer_mean = stat_method.statistics.peer_mean
                                peer_std = stat_method.statistics.peer_std or 1.0
                                if peer_std > 0:
                                    z_scores = [(v - peer_mean) / peer_std for v in timeline_values]
                                    return z_scores
                    # Fallback: return timeline values as proxy
                    timeline_values = metric.get_timeline_values()
                    if timeline_values:
                        return timeline_values

        return None

    def parse_node_results(self, path: Path) -> list[SignalCreate]:
        """Parse a node results JSONL file and extract signals.

        Reads a node results JSONL file, validates its structure using Pydantic,
        and extracts SignalCreate objects for each entity with anomalies.

        Args:
            path: Path to the node results JSONL file.

        Returns:
            list[SignalCreate]: List of signals extracted from the node results.
                Empty list if no signals meet the anomaly threshold.

        Raises:
            SignalGeneratorError: If the file cannot be read or has invalid format.
            FileNotFoundError: If the path does not exist.

        Example:
            >>> generator = SignalGenerator()
            >>> signals = generator.parse_node_results(
            ...     Path("fixtures/runs/test/results/nodes/losIndex__medicareId.jsonl")
            ... )
            >>> len(signals) > 0
            True
        """
        if not path.exists():
            raise FileNotFoundError(f"Node results file not found: {path}")

        try:
            data = _load_node_jsonl(path)
        except (json.JSONDecodeError, ValueError) as e:
            raise SignalGeneratorError(f"Invalid JSONL format: {e}", path=path) from e
        except OSError as e:
            raise SignalGeneratorError(f"Failed to read file: {e}", path=path) from e

        try:
            node_results = NodeResults.model_validate(data)
        except (ValidationError, ValueError) as e:
            raise SignalGeneratorError(f"Failed to validate node results schema: {e}", path=path) from e

        return self._extract_signals(node_results, path)

    def parse_node_results_from_dict(self, data: dict[str, object], path: Path | None = None) -> list[SignalCreate]:
        """Parse node results from a dictionary.

        Useful for testing or when data is already loaded.

        Args:
            data: Dictionary containing node results data.
            path: Optional path for error context and result metadata.

        Returns:
            list[SignalCreate]: List of signals extracted from the node results.

        Raises:
            SignalGeneratorError: If the data has invalid format.

        Example:
            >>> generator = SignalGenerator()
            >>> data = {"canonical_node_id": "...", "entity_results": [...]}
            >>> signals = generator.parse_node_results_from_dict(data)
        """
        try:
            node_results = NodeResults.model_validate(data)
        except (ValidationError, ValueError) as e:
            raise SignalGeneratorError(f"Failed to validate node results schema: {e}", path=path) from e

        return self._extract_signals(node_results, path)

    def _extract_signals(self, node_results: NodeResults, path: Path | None) -> list[SignalCreate]:
        """Extract signals from validated node results.

        Loads temporal node data via trends_to edge if available, then
        extracts signals with classification for each entity.

        Args:
            node_results: Validated NodeResults object.
            path: Optional path for metadata.

        Returns:
            list[SignalCreate]: List of extracted signals.
        """
        signals: list[SignalCreate] = []
        detected_at = datetime.now(tz=UTC)

        # Get temporal node ID via trends_to edge
        temporal_node_id = node_results.get_temporal_node_id()
        temporal_node: NodeResults | None = None

        if temporal_node_id:
            temporal_node = self._load_temporal_node(temporal_node_id)

        for entity_result in node_results.entity_results:
            signal = self._extract_signal_from_entity(
                entity_result=entity_result,
                canonical_node_id=node_results.canonical_node_id,
                detected_at=detected_at,
                path=path,
                temporal_node_id=temporal_node_id,
                temporal_node=temporal_node,
            )
            if signal is not None:
                signals.append(signal)

        logger.info(
            "Extracted %d signals from node %s",
            len(signals),
            node_results.canonical_node_id,
        )
        return signals

    def _extract_signal_from_entity(
        self,
        entity_result: NodeEntityResult,
        canonical_node_id: str,
        detected_at: datetime,
        path: Path | None,
        temporal_node_id: str | None = None,
        temporal_node: NodeResults | None = None,
    ) -> SignalCreate | None:
        """Extract a signal from a single entity result.

        Args:
            entity_result: The entity result to process.
            canonical_node_id: The canonical node ID.
            detected_at: Detection timestamp.
            path: Optional path for metadata.
            temporal_node_id: Optional ID of linked temporal node.
            temporal_node: Optional parsed temporal node data.

        Returns:
            SignalCreate if an anomaly signal should be generated, None otherwise.
        """
        # Get metric info
        if not entity_result.metric:
            return None
        metric = entity_result.metric[0]
        metric_id = metric.metadata.metric_id

        # Skip entities with null metric values (suppressed data)
        if metric.values is None:
            return None

        # Handle temporal timeline values - use the latest value
        if isinstance(metric.values, TemporalTimeline):
            if metric.values.timeline:
                metric_value = Decimal(str(metric.values.timeline[-1].value))
            else:
                return None
        else:
            metric_value = Decimal(str(metric.values))

        # Find the most severe anomaly across all statistical methods
        best_anomaly = self._find_best_anomaly(entity_result.statistical_methods)
        if best_anomaly is None:
            return None

        anomaly_level, interpretation, statistics = best_anomaly

        # Check if we should generate a signal for this anomaly level
        if anomaly_level == "normal" and not self.include_normal:
            return None

        if anomaly_level not in SIGNAL_ANOMALY_LEVELS and not self.include_normal:
            return None

        # Map metric to domain
        domain = METRIC_TO_DOMAIN.get(metric_id, DEFAULT_DOMAIN)

        # Extract entity fields
        facility, service_line, sub_service_line = self._extract_entity_fields(entity_result)

        # Temporal data (classification is now handled by dbt via simplified signal types)
        monthly_z_scores: list[float] | None = None
        slope_percentile: Decimal | None = None

        if temporal_node is not None:
            entity_key = (facility, service_line, sub_service_line)
            monthly_z_scores = self._get_temporal_z_scores(temporal_node, entity_key)

        return SignalCreate(
            canonical_node_id=canonical_node_id,
            metric_id=metric_id,
            domain=domain,
            facility=facility,
            facility_id=facility,  # Use facility as ID for now
            service_line=service_line,
            sub_service_line=sub_service_line,
            description=interpretation,
            metric_value=metric_value,
            peer_mean=Decimal(str(statistics.peer_mean)) if statistics.peer_mean else None,
            percentile_rank=Decimal(str(statistics.percentile_rank)) if statistics.percentile_rank else None,
            simple_zscore=Decimal(str(statistics.simple_zscore)) if statistics.simple_zscore else None,
            encounters=entity_result.encounters,
            detected_at=detected_at,
            temporal_node_id=temporal_node_id,
            slope_percentile=slope_percentile,
            monthly_z_scores=monthly_z_scores,
        )

    def _find_best_anomaly(self, statistical_methods: list[NodeStatisticalMethod]) -> tuple[str, str, NodeStatistics] | None:
        """Find the most significant anomaly across all statistical methods.

        Prioritizes simple_zscore method, then looks for the highest significance
        anomaly across all methods.

        Args:
            statistical_methods: List of statistical method results.

        Returns:
            Tuple of (anomaly_level, interpretation_text, statistics) for the
            most significant anomaly, or None if no anomalies found.
        """
        best_significance_rank: float = 999  # Lower is more significant
        best_result: tuple[str, str, NodeStatistics] | None = None

        significance_rank = {
            "extremely_high": 0,
            "extremely_low": 0,
            "very_high": 1,
            "very_low": 1,
            "moderately_high": 2,
            "moderately_low": 2,
            "slightly_high": 3,
            "slightly_low": 3,
            "normal": 4,
        }

        for stat_method in statistical_methods:
            # Prefer simple_zscore method
            is_simple_zscore = "simple_zscore" in stat_method.statistical_method

            for anomaly in stat_method.anomalies:
                for method in anomaly.methods:
                    anomaly_level = method.anomaly
                    rank = significance_rank.get(anomaly_level, 5)

                    # Give bonus to simple_zscore method (subtract 0.5 from rank)
                    effective_rank = rank - (0.5 if is_simple_zscore else 0)

                    if effective_rank < best_significance_rank:
                        best_significance_rank = effective_rank
                        best_result = (
                            anomaly_level,
                            method.interpretation.rendered,
                            stat_method.statistics,
                        )

        return best_result

    def _extract_entity_fields(self, entity_result: NodeEntityResult) -> tuple[str, str, str | None]:
        """Extract facility, service line, and sub-service line from entity.

        Args:
            entity_result: The entity result containing entity fields.

        Returns:
            Tuple of (facility, service_line, sub_service_line).
            Facility defaults to "Unknown" if not found.
            Service line defaults to "All" if not found.
            Sub-service line is None if not found.
        """
        facility = "Unknown"
        service_line = "All"
        sub_service_line: str | None = None

        for entity_field in entity_result.entity:
            if entity_field.id == "medicareId":
                facility = entity_field.value
            elif entity_field.id == "vizientServiceLine":
                service_line = entity_field.value
            elif entity_field.id == "vizientSubServiceLine":
                sub_service_line = entity_field.value

        return facility, service_line, sub_service_line
