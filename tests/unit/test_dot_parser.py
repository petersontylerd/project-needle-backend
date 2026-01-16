"""Unit tests for DotParserService."""

from pathlib import Path
from textwrap import dedent

import pytest

from src.runs.services.dot_parser import DotParserService


class TestDotParserService:
    """Tests for DOT file parsing."""

    @pytest.fixture
    def sample_dot_content(self) -> str:
        """Sample DOT file content."""
        return dedent("""
            digraph insight_graph {
              rankdir=LR;
              "nodeA" [shape="box", style="rounded,filled", fillcolor="#FFFFFF", color="#696969", fontcolor="#696969", label="metric1
            facet1
            grain1"];
              "nodeB" [shape="box", style="rounded,filled", fillcolor="#FFFFFF", color="#696969", fontcolor="#696969", label="metric1
            facet1
            grain2"];
              "nodeA" -> "nodeB" [color="#565EAA", style="dashed", penwidth="1.4", xlabel="trends_to"];
            }
        """).strip()

    @pytest.fixture
    def dot_file(self, tmp_path: Path, sample_dot_content: str) -> Path:
        """Create temporary DOT file."""
        dot_path = tmp_path / "test.dot"
        dot_path.write_text(sample_dot_content)
        return dot_path

    def test_parse_nodes(self, dot_file: Path) -> None:
        """Test parsing nodes from DOT file."""
        service = DotParserService()
        result = service.parse_file(dot_file)

        assert len(result.nodes) == 2
        node_ids = {n.id for n in result.nodes}
        assert "nodeA" in node_ids
        assert "nodeB" in node_ids

    def test_parse_node_label(self, dot_file: Path) -> None:
        """Test that node labels are extracted."""
        service = DotParserService()
        result = service.parse_file(dot_file)

        node_a = next(n for n in result.nodes if n.id == "nodeA")
        assert "metric1" in node_a.label
        assert node_a.shape == "box"

    def test_parse_edges(self, dot_file: Path) -> None:
        """Test parsing edges from DOT file."""
        service = DotParserService()
        result = service.parse_file(dot_file)

        assert len(result.edges) == 1
        edge = result.edges[0]
        assert edge.source == "nodeA"
        assert edge.target == "nodeB"
        assert edge.label == "trends_to"
        assert edge.dashes is True  # trends_to should be dashed

    def test_parse_file_not_found(self, tmp_path: Path) -> None:
        """Test handling of missing file."""
        service = DotParserService()
        result = service.parse_file(tmp_path / "nonexistent.dot")

        assert len(result.nodes) == 0
        assert len(result.edges) == 0
