"""Tests for Cypher query builder utilities."""

from src.ontology.cypher import (
    _build_properties,
    _escape_value,
    age_query_wrapper,
    build_create_edge,
    build_create_vertex,
    build_match_vertex,
    build_traverse,
)


class TestEscapeValue:
    """Tests for _escape_value function."""

    def test_escape_none(self) -> None:
        """None should become null."""
        assert _escape_value(None) == "null"

    def test_escape_bool_true(self) -> None:
        """True should become true."""
        assert _escape_value(True) == "true"

    def test_escape_bool_false(self) -> None:
        """False should become false."""
        assert _escape_value(False) == "false"

    def test_escape_int(self) -> None:
        """Integers should be unquoted."""
        assert _escape_value(42) == "42"

    def test_escape_float(self) -> None:
        """Floats should be unquoted."""
        assert _escape_value(3.14) == "3.14"

    def test_escape_string(self) -> None:
        """Strings should be quoted."""
        assert _escape_value("hello") == "'hello'"

    def test_escape_string_with_quotes(self) -> None:
        """Single quotes should be escaped."""
        assert _escape_value("it's") == "'it\\'s'"

    def test_escape_string_with_backslash(self) -> None:
        """Backslashes should be escaped."""
        assert _escape_value("a\\b") == "'a\\\\b'"

    def test_escape_string_with_newline(self) -> None:
        """Newlines should be escaped."""
        assert _escape_value("line1\nline2") == "'line1\\nline2'"

    def test_escape_string_with_carriage_return(self) -> None:
        """Carriage returns should be escaped."""
        assert _escape_value("a\rb") == "'a\\rb'"

    def test_escape_string_with_tab(self) -> None:
        """Tabs should be escaped."""
        assert _escape_value("col1\tcol2") == "'col1\\tcol2'"


class TestBuildProperties:
    """Tests for _build_properties function."""

    def test_empty_properties(self) -> None:
        """Empty dict should return empty string."""
        assert _build_properties({}) == ""

    def test_single_property(self) -> None:
        """Single property should be formatted."""
        result = _build_properties({"id": "F1"})
        assert result == "{id: 'F1'}"

    def test_multiple_properties(self) -> None:
        """Multiple properties should be comma-separated."""
        result = _build_properties({"id": "F1", "name": "Hospital"})
        assert "id: 'F1'" in result
        assert "name: 'Hospital'" in result


class TestBuildCreateVertex:
    """Tests for build_create_vertex function."""

    def test_create_facility(self) -> None:
        """Should create Facility vertex."""
        cypher = build_create_vertex("Facility", {"id": "F1", "name": "Hospital"})
        assert "CREATE (n:Facility" in cypher
        assert "id: 'F1'" in cypher
        assert "RETURN n" in cypher


class TestBuildCreateEdge:
    """Tests for build_create_edge function."""

    def test_create_has_signal_edge(self) -> None:
        """Should create edge between Facility and Signal."""
        cypher = build_create_edge("Facility", "F1", "has_signal", "Signal", "S1")
        assert "MATCH (a:Facility" in cypher
        assert "MATCH" in cypher and "Signal" in cypher
        assert "CREATE (a)-[r:has_signal]->(b)" in cypher
        assert "RETURN r" in cypher


class TestBuildMatchVertex:
    """Tests for build_match_vertex function."""

    def test_match_by_label(self) -> None:
        """Should match vertices by label."""
        cypher = build_match_vertex("Facility")
        assert "MATCH (n:Facility" in cypher
        assert "RETURN n" in cypher

    def test_match_with_filter(self) -> None:
        """Should include filter properties."""
        cypher = build_match_vertex("Facility", {"id": "F1"})
        assert "id: 'F1'" in cypher

    def test_match_with_limit(self) -> None:
        """Should include LIMIT clause."""
        cypher = build_match_vertex("Facility", limit=10)
        assert "LIMIT 10" in cypher


class TestBuildTraverse:
    """Tests for build_traverse function."""

    def test_traverse_outgoing(self) -> None:
        """Should traverse outgoing edges."""
        cypher = build_traverse("Facility", "F1", "has_signal", "outgoing")
        assert "MATCH (n:Facility" in cypher
        assert "-[r:has_signal]->" in cypher
        assert "RETURN m, r" in cypher

    def test_traverse_incoming(self) -> None:
        """Should traverse incoming edges."""
        cypher = build_traverse("Signal", "S1", "has_signal", "incoming")
        assert "<-[r:has_signal]-" in cypher

    def test_traverse_both(self) -> None:
        """Should traverse both directions."""
        cypher = build_traverse("Signal", "S1", direction="both")
        assert "-[r]-" in cypher

    def test_traverse_any_edge(self) -> None:
        """Should traverse any edge type when edge_label is None."""
        cypher = build_traverse("Facility", "F1", edge_label=None)
        assert "-[r]->" in cypher


class TestAgeQueryWrapper:
    """Tests for age_query_wrapper function."""

    def test_wraps_cypher_in_sql(self) -> None:
        """Should wrap Cypher in AGE SQL function."""
        sql = age_query_wrapper("MATCH (n) RETURN n")
        assert "SELECT * FROM cypher(" in sql
        assert "'healthcare_ontology'" in sql
        assert "MATCH (n) RETURN n" in sql
        assert "AS (result agtype)" in sql

    def test_custom_graph_name(self) -> None:
        """Should use custom graph name."""
        sql = age_query_wrapper("MATCH (n) RETURN n", "custom_graph")
        assert "'custom_graph'" in sql

    def test_custom_columns(self) -> None:
        """Should accept custom column specification."""
        sql = age_query_wrapper(
            "MATCH (n)-[r]->(m) RETURN m, r",
            columns="(m agtype, r agtype)",
        )
        assert "AS (m agtype, r agtype)" in sql

    def test_default_columns(self) -> None:
        """Should use default single column specification."""
        sql = age_query_wrapper("MATCH (n) RETURN n")
        assert "AS (result agtype)" in sql
