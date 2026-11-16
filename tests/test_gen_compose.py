"""Unit tests for cluster/gen_compose.py (no Docker required).

Verifies:
- Output is valid YAML (parsed with PyYAML or manual check)
- N services generated for N nodes
- Each node has PEERS without itself
- HOST=0.0.0.0 present for every service
- Correct port mappings (5000+i)
- Bind-mount volumes ./data/nodeX:/data
- Network name is crdt-net
- N out of range [4..10] -> exits with code 2
"""

from __future__ import annotations

import io
import os
import sys
import tempfile
from pathlib import Path

import pytest

# Allow imports from the project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from cluster.gen_compose import main as gen_main


def _generate(n: int, **kwargs) -> str:
    """Run gen_compose and return generated YAML as a string."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False, encoding="utf-8"
    ) as f:
        tmp_path = f.name

    try:
        extra = []
        for k, v in kwargs.items():
            extra += [f"--{k.replace('_', '-')}", str(v)]
        gen_main(["--nodes", str(n), "--output", tmp_path] + extra)
        return Path(tmp_path).read_text(encoding="utf-8")
    finally:
        os.unlink(tmp_path)


class TestGenCompose:
    """Tests for the compose file generator."""

    @pytest.mark.parametrize("n", [4, 6, 10])
    def test_generates_n_services(self, n: int) -> None:
        """Generated YAML must contain exactly N service blocks."""
        content = _generate(n)
        # Count service definitions: each node starts with "  nodeX:"
        service_count = sum(
            1 for line in content.splitlines()
            if line.startswith("  node") and line.rstrip().endswith(":")
        )
        assert service_count == n, f"Expected {n} services, found {service_count}"

    @pytest.mark.parametrize("n", [4, 6, 10])
    def test_peers_exclude_self(self, n: int) -> None:
        """Each node's PEERS list must not contain its own node_id."""
        content = _generate(n)
        lines = content.splitlines()

        current_node: str | None = None
        for line in lines:
            if line.startswith("  node") and line.rstrip().endswith(":"):
                current_node = line.strip().rstrip(":")
            elif current_node and "PEERS:" in line:
                peers_str = line.split("PEERS:", 1)[1].strip().strip("'\"")
                peer_ids = [
                    p.split("=")[0]
                    for p in peers_str.split(",")
                    if "=" in p
                ]
                assert current_node not in peer_ids, (
                    f"{current_node} must not appear in its own PEERS"
                )

    @pytest.mark.parametrize("n", [4, 6, 10])
    def test_host_is_0_0_0_0(self, n: int) -> None:
        """Every service must have HOST=0.0.0.0 in its environment."""
        content = _generate(n)
        host_lines = [l.strip() for l in content.splitlines() if "HOST:" in l]
        assert len(host_lines) == n, f"Expected {n} HOST entries, found {len(host_lines)}"
        for line in host_lines:
            assert "0.0.0.0" in line, f"HOST must be 0.0.0.0, got: {line}"

    @pytest.mark.parametrize("n,node_i,expected_port", [
        (4, 1, 5001),
        (4, 4, 5004),
        (6, 3, 5003),
        (10, 10, 5010),
    ])
    def test_port_mapping(self, n: int, node_i: int, expected_port: int) -> None:
        """Port mapping must be 5000+i for node i."""
        content = _generate(n)
        port_line = f"'{expected_port}:5000'"
        assert port_line in content, (
            f"Expected port mapping {port_line} not found in compose for n={n}"
        )

    @pytest.mark.parametrize("n", [4, 6])
    def test_bind_mount_volumes(self, n: int) -> None:
        """Each node must have a ./data/nodeX:/data volume bind-mount."""
        content = _generate(n)
        for i in range(1, n + 1):
            expected = f"./data/node{i}:/data"
            assert expected in content, (
                f"Volume bind-mount '{expected}' not found in compose"
            )

    @pytest.mark.parametrize("n", [4, 6])
    def test_network_name_crdt_net(self, n: int) -> None:
        """The external network must be named 'crdt-net'."""
        content = _generate(n)
        assert "name: crdt-net" in content, "Network must be named 'crdt-net'"
        assert "crdt-net:" in content, "Network 'crdt-net' must be declared under networks:"

    @pytest.mark.parametrize("n", [4, 6])
    def test_no_version_key(self, n: int) -> None:
        """The generated file must NOT have an obsolete top-level 'version:' key."""
        content = _generate(n)
        top_lines = content.splitlines()[:5]
        for line in top_lines:
            assert not line.strip().startswith("version:"), (
                "Obsolete 'version:' key must not be present"
            )

    @pytest.mark.parametrize("n", [4, 6])
    def test_has_generated_comment(self, n: int) -> None:
        """First line must be the 'generated, do not edit' comment."""
        content = _generate(n)
        first_line = content.splitlines()[0]
        assert "Generated" in first_line or "generated" in first_line, (
            f"First line must be a 'generated' comment, got: {first_line!r}"
        )

    @pytest.mark.parametrize("n", [4, 6])
    def test_top_level_name_crdt_tracker(self, n: int) -> None:
        """Top-level compose 'name:' must be 'crdt-tracker'."""
        content = _generate(n)
        lines = content.splitlines()
        name_lines = [l for l in lines if l.startswith("name:")]
        assert name_lines, "Top-level 'name:' key missing"
        assert "crdt-tracker" in name_lines[0], (
            f"Expected 'crdt-tracker', got: {name_lines[0]}"
        )

    @pytest.mark.parametrize("n", [4, 6])
    def test_healthcheck_present(self, n: int) -> None:
        """Every service must include a healthcheck."""
        content = _generate(n)
        hc_count = content.count("healthcheck:")
        assert hc_count == n, f"Expected {n} healthcheck blocks, found {hc_count}"

    @pytest.mark.parametrize("n", [4, 6])
    def test_init_true_present(self, n: int) -> None:
        """Every service must have init: true."""
        content = _generate(n)
        init_count = sum(1 for l in content.splitlines() if "init: true" in l)
        assert init_count == n, f"Expected {n} 'init: true' entries, found {init_count}"

    # Out-of-range N tests
    @pytest.mark.parametrize("n", [0, 1, 3, 11, 100])
    def test_n_out_of_range_exits_with_code_2(self, n: int) -> None:
        """N outside [4..10] must cause SystemExit with code 2."""
        with tempfile.NamedTemporaryFile(suffix=".yml", delete=False) as f:
            tmp_path = f.name
        try:
            with pytest.raises(SystemExit) as exc_info:
                gen_main(["--nodes", str(n), "--output", tmp_path])
            assert exc_info.value.code == 2, (
                f"Expected exit code 2 for n={n}, got {exc_info.value.code}"
            )
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    @pytest.mark.parametrize("n", [4, 6])
    def test_valid_yaml(self, n: int) -> None:
        """Generated output must be parseable as valid YAML."""
        content = _generate(n)
        try:
            import yaml
            parsed = yaml.safe_load(content)
            assert parsed is not None
            assert "services" in parsed
        except ImportError:
            # PyYAML not installed: do basic structural validation instead
            assert "services:" in content
            assert "networks:" in content
