"""Cypher query builder utilities for Apache AGE.

This module provides functions to build Cypher queries for the healthcare_ontology
graph. Apache AGE requires Cypher to be wrapped in SQL function calls.

Usage:
    cypher = build_create_vertex("Facility", {"id": "F1", "name": "Hospital"})
    sql = age_query_wrapper(cypher, "healthcare_ontology")
    # Execute sql via SQLAlchemy connection
"""

from typing import Any

from .schema import GRAPH_NAME


def _escape_value(value: Any) -> str:
    """Escape a value for Cypher query.

    Args:
        value: The value to escape.

    Returns:
        Properly escaped string for Cypher.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    # String: escape backslashes, single quotes, and control characters
    escaped = str(value)
    escaped = escaped.replace("\\", "\\\\")  # Backslash first
    escaped = escaped.replace("'", "\\'")
    escaped = escaped.replace("\n", "\\n")
    escaped = escaped.replace("\r", "\\r")
    escaped = escaped.replace("\t", "\\t")
    return f"'{escaped}'"


def _build_properties(properties: dict[str, Any]) -> str:
    """Build Cypher property map from dictionary.

    Args:
        properties: Dictionary of property names to values.

    Returns:
        Cypher property map string like {id: 'F1', name: 'Hospital'}.
    """
    if not properties:
        return ""
    pairs = [f"{key}: {_escape_value(value)}" for key, value in properties.items()]
    return "{" + ", ".join(pairs) + "}"


def build_create_vertex(label: str, properties: dict[str, Any]) -> str:
    """Build Cypher CREATE statement for a vertex.

    Args:
        label: Vertex label (e.g., 'Facility', 'Signal').
        properties: Dictionary of vertex properties.

    Returns:
        Cypher CREATE statement.

    Example:
        >>> build_create_vertex("Facility", {"id": "F1", "name": "Hospital"})
        "CREATE (n:Facility {id: 'F1', name: 'Hospital'}) RETURN n"
    """
    props = _build_properties(properties)
    return f"CREATE (n:{label} {props}) RETURN n"


def build_create_edge(
    from_label: str,
    from_id: str,
    edge_label: str,
    to_label: str,
    to_id: str,
    properties: dict[str, Any] | None = None,
) -> str:
    """Build Cypher CREATE statement for an edge.

    Args:
        from_label: Source vertex label.
        from_id: Source vertex id property.
        edge_label: Edge relationship label.
        to_label: Target vertex label.
        to_id: Target vertex id property.
        properties: Optional edge properties.

    Returns:
        Cypher MATCH/CREATE statement for the edge.

    Example:
        >>> build_create_edge("Facility", "F1", "has_signal", "Signal", "S1")
        "MATCH (a:Facility {id: 'F1'}), (b:Signal {id: 'S1'}) CREATE (a)-[r:has_signal]->(b) RETURN r"
    """
    props = _build_properties(properties) if properties else ""
    return (
        f"MATCH (a:{from_label} {{id: {_escape_value(from_id)}}}), "
        f"(b:{to_label} {{id: {_escape_value(to_id)}}}) "
        f"CREATE (a)-[r:{edge_label}{props}]->(b) RETURN r"
    )


def build_match_vertex(
    label: str,
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
) -> str:
    """Build Cypher MATCH statement for vertices.

    Args:
        label: Vertex label to match.
        filters: Optional property filters.
        limit: Optional result limit.

    Returns:
        Cypher MATCH statement.

    Example:
        >>> build_match_vertex("Facility", {"id": "F1"})
        "MATCH (n:Facility {id: 'F1'}) RETURN n"
    """
    props = _build_properties(filters) if filters else ""
    query = f"MATCH (n:{label} {props}) RETURN n"
    if limit is not None:
        query += f" LIMIT {limit}"
    return query


def build_traverse(
    start_label: str,
    start_id: str,
    edge_label: str | None = None,
    direction: str = "outgoing",
    depth: int = 1,
) -> str:
    """Build Cypher traversal query from a starting vertex.

    Args:
        start_label: Starting vertex label.
        start_id: Starting vertex id property.
        edge_label: Optional edge label to filter (None for any edge).
        direction: 'outgoing', 'incoming', or 'both'.
        depth: Maximum traversal depth (default 1).

    Returns:
        Cypher traversal query.

    Example:
        >>> build_traverse("Facility", "F1", "has_signal", "outgoing", 1)
        "MATCH (n:Facility {id: 'F1'})-[r:has_signal]->(m) RETURN m, r"
    """
    edge_spec = f":{edge_label}" if edge_label else ""
    depth_spec = f"*1..{depth}" if depth > 1 else ""

    if direction == "outgoing":
        pattern = f"-[r{edge_spec}{depth_spec}]->"
    elif direction == "incoming":
        pattern = f"<-[r{edge_spec}{depth_spec}]-"
    else:  # both
        pattern = f"-[r{edge_spec}{depth_spec}]-"

    return f"MATCH (n:{start_label} {{id: {_escape_value(start_id)}}}){pattern}(m) RETURN m, r"


def age_query_wrapper(
    cypher: str,
    graph_name: str = GRAPH_NAME,
    columns: str = "(result agtype)",
) -> str:
    """Wrap Cypher query in AGE SQL function call.

    Apache AGE requires Cypher queries to be executed via the cypher() function.

    Args:
        cypher: The Cypher query string.
        graph_name: Name of the graph (default: healthcare_ontology).
        columns: Column specification for the result (default: single agtype column).

    Returns:
        SQL statement to execute the Cypher query.

    Example:
        >>> age_query_wrapper("MATCH (n) RETURN n", "my_graph")
        "SELECT * FROM cypher('my_graph', $$ MATCH (n) RETURN n $$) AS (result agtype)"
        >>> age_query_wrapper("MATCH (n)-[r]->(m) RETURN m, r", columns="(m agtype, r agtype)")
        "SELECT * FROM cypher('healthcare_ontology', $$ ... $$) AS (m agtype, r agtype)"
    """
    return f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS {columns}"
