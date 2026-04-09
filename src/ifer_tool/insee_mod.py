"""INSEE public facade with 4 main functions."""
from __future__ import annotations

from pathlib import Path

from ifer_tool.duckdb_adapter import open_duckdb_connection
from ifer_tool.insee_build_core import _build_cog_tuu_tduu_table
from ifer_tool.insee_discovery import (
    _download_file,
    _probe_fichier_endpoints,
    _probe_known_cog_links,
    _probe_known_history_links,
    _select_link_for_year,
)
from ifer_tool.insee_types import (
    InseeArtifact,
    InseeBuildResult,
    InseeError,
    ensure_insee_dir,
)


def resolve_cog_file(output_dir: Path, target_year: int) -> InseeArtifact:
    """Download COG (communes) file."""
    ensure_insee_dir(output_dir)
    links = _probe_known_cog_links(target_year)
    if not links:
        links = _probe_fichier_endpoints(("2521852", "2560452", "8377162", "13226", "13213", "13212", "13211"))
    if not links:
        raise InseeError(f"No COG file found for {target_year}")
    url, year = _select_link_for_year(links, target_year, ("csv", "xlsx", "xls", "zip"), ("commune", "cog", "v_commune"))
    path = _download_file(url, output_dir)
    return InseeArtifact(path=path, year=year or target_year, source_url=url)


def resolve_uu2020_file(output_dir: Path, target_year: int) -> InseeArtifact:
    """Download UU2020 (unités urbaines) file."""
    ensure_insee_dir(output_dir)
    links = _probe_known_cog_links(target_year)
    if not links:
        links = _probe_fichier_endpoints(("8375723", "8375721", "8375719", "2521852", "2560452", "8377162"))
    if not links:
        raise InseeError(f"No UU2020 file found for {target_year}")
    url, year = _select_link_for_year(links, target_year, ("csv", "xlsx", "xls", "zip"), ("urban", "uu", "unite", "urbaine"))
    path = _download_file(url, output_dir)
    return InseeArtifact(path=path, year=year or target_year, source_url=url)


def resolve_history_file(output_dir: Path) -> InseeArtifact:
    """Download historical communes file (since 1943)."""
    ensure_insee_dir(output_dir)
    links = _probe_known_history_links(2025)
    if not links:
        raise InseeError("No history file found")
    url, year = _select_link_for_year(links, 2025, ("csv", "dbf"), ("histoire", "historique", "depuis", "1943", "mouvement"))
    path = _download_file(url, output_dir)
    return InseeArtifact(path=path, year=year or 2025, source_url=url)


def build_insee_duckdb_table(database_path: Path, cog_file: InseeArtifact, uu2020_file: InseeArtifact | None = None, history_file: InseeArtifact | None = None, target_year: int = 2025, metro_only: bool = True) -> InseeBuildResult:
    """Build INSEE DuckDB table."""
    con = open_duckdb_connection(database_path)
    if history_file is None:
        history_file = InseeArtifact(path=Path(__file__).resolve().parents[2] / "insee" / "historique_communes_fallback.csv", year=2025, source_url="fallback")
    if uu2020_file is None:
        uu2020_file = cog_file
    tbl, cnt = _build_cog_tuu_tduu_table(con, cog_file.path, uu2020_file.path, history_file.path, target_year, metro_only)
    return InseeBuildResult(table_name=tbl, row_count=cnt, database_path=database_path)
