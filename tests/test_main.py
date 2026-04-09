
import zipfile
from pathlib import Path
import os
import gzip
from io import BytesIO

import duckdb
import pytest

from ifer_tool.data_gouv import DataGouvError, fetch_reference_tables, find_reference_resources
from ifer_tool.duckdb_adapter import load_anfr_archives_to_duckdb
from ifer_tool.insee_mod import (
    build_insee_duckdb_table,
    resolve_cog_file,
    resolve_history_file,
    resolve_uu2020_file,
)
from ifer_tool.insee_types import InseeArtifact, InseeError
from ifer_tool.insee_build_core import _prepare_tabular_file
from ifer_tool.main import (
    DEFAULT_DUCKDB_PATH,
    DEFAULT_EXTRACT_DIR,
    DEFAULT_INSEE_DIR,
    DEFAULT_INSEE_DUCKDB_PATH,
    DEFAULT_OUTPUT_DIR,
    build_parser,
)


def test_find_reference_resources_filters_2026_reference_tables() -> None:
    dataset = {
        "title": "Installations radioélectriques > 5W",
        "resources": [
            {"title": "Table de références 2025", "url": "https://example.test/ref-2025.csv"},
            {"title": "Cartographie 2026", "url": "https://example.test/map-2026.csv"},
            {"title": "Table de références 2026", "url": "https://example.test/ref-2026.csv"},
        ],
    }

    resources = find_reference_resources(dataset, year=2026)

    assert [resource["title"] for resource in resources] == ["Table de références 2026"]


def test_find_reference_resources_raises_when_missing() -> None:
    dataset = {"title": "Installations radioélectriques > 5W", "resources": []}

    with pytest.raises(DataGouvError, match="Aucune table de références 2026"):
        find_reference_resources(dataset, year=2026)


def test_fetch_reference_tables_downloads_matching_resources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = {
        "title": "Installations radioélectriques > 5W",
        "resources": [
            {
                "title": "Table de références 2026",
                "url": "https://example.test/ref-2026.csv",
                "format": "csv",
            }
        ],
    }

    monkeypatch.setattr("ifer_tool.data_gouv.find_target_dataset", lambda query: dataset)

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return b"colonne\nvaleur\n"

    monkeypatch.setattr("ifer_tool.data_gouv.urlopen", lambda url: FakeResponse())

    downloaded_files = fetch_reference_tables(output_dir=tmp_path)

    assert len(downloaded_files) == 1
    assert downloaded_files[0].read_text() == "colonne\nvaleur\n"
    assert downloaded_files[0].name == "table-de-references-2026.csv"


def test_build_parser_uses_2026_defaults() -> None:
    args = build_parser().parse_args([])

    assert args.command == "fetch"
    assert args.year == 2026
    assert args.output_dir == DEFAULT_OUTPUT_DIR


def test_build_parser_load_duckdb_defaults() -> None:
    args = build_parser().parse_args(["load-duckdb"])

    assert args.command == "load-duckdb"
    assert args.source_dir == DEFAULT_OUTPUT_DIR
    assert args.database_path == DEFAULT_DUCKDB_PATH
    assert args.extract_dir == DEFAULT_EXTRACT_DIR


def test_build_parser_insee_defaults() -> None:
    args = build_parser().parse_args(["insee-build"])

    assert args.command == "insee-build"
    assert args.insee_dir == DEFAULT_INSEE_DIR
    assert args.insee_database_path == DEFAULT_INSEE_DUCKDB_PATH
    assert args.insee_year == 2025
    assert args.expected_tolerance == 0


def test_resolve_cog_file_uses_previous_year_when_target_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    search_calls: list[str] = []

    def fake_search_page(query: str) -> str:
        search_calls.append(query)
        if "2025" in query:
            return "<html></html>"
        return '<a href="/fr/statistiques/fichier/123456/cog_2024.csv">cog 2024</a>'

    monkeypatch.setattr("ifer_tool.insee_mod._search_page", fake_search_page)
    monkeypatch.setattr("ifer_tool.insee_mod._search_web_pages_for_insee", lambda query: [])
    monkeypatch.setattr("ifer_tool.insee_mod._extract_stat_file_links_from_info_page", lambda page_url: [])
    monkeypatch.setattr("ifer_tool.insee_mod._probe_fichier_endpoints", lambda product_ids: [])
    monkeypatch.setattr("ifer_tool.insee_mod._probe_known_cog_links", lambda target_year: [])
    monkeypatch.setattr(
        "ifer_tool.insee_mod._download_file",
        lambda url, destination_dir: destination_dir / Path(url).name,
    )

    artifact = resolve_cog_file(output_dir=tmp_path, target_year=2025)

    assert artifact.year == 2024
    assert artifact.url.endswith("cog_2024.csv")
    assert any("2025" in call for call in search_calls)
    assert any("2024" in call for call in search_calls)


def test_resolve_cog_file_uses_web_fallback_when_search_is_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ifer_tool.insee_mod._search_page", lambda query: "<html></html>")
    monkeypatch.setattr("ifer_tool.insee_mod._extract_stat_file_links_from_info_page", lambda page_url: [])
    monkeypatch.setattr("ifer_tool.insee_mod._probe_fichier_endpoints", lambda product_ids: [])
    monkeypatch.setattr("ifer_tool.insee_mod._probe_known_cog_links", lambda target_year: [])
    monkeypatch.setattr(
        "ifer_tool.insee_mod._search_web_pages_for_insee",
        lambda query: ["https://www.insee.fr/fr/statistiques/9999999"],
    )
    monkeypatch.setattr(
        "ifer_tool.insee_mod._fetch_page",
        lambda url: '<a href="/fr/statistiques/fichier/8888888/cog_2025.csv">fichier</a>',
    )
    monkeypatch.setattr(
        "ifer_tool.insee_mod._download_file",
        lambda url, destination_dir: destination_dir / Path(url).name,
    )

    artifact = resolve_cog_file(output_dir=tmp_path, target_year=2025)

    assert artifact.year == 2025
    assert artifact.url.endswith("cog_2025.csv")


def test_resolve_uu2020_file_uses_secondary_query_when_first_is_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    monkeypatch.setattr("ifer_tool.insee_mod._extract_stat_file_links_from_info_page", lambda page_url: [])

    def fake_collect(query: str, max_result_pages: int = 8) -> list[str]:
        calls.append(query)
        if len(calls) == 1:
            return []
        return ["https://www.insee.fr/fr/statistiques/fichier/7777777/uu2020_2025.csv"]

    monkeypatch.setattr("ifer_tool.insee_mod._collect_candidate_links", fake_collect)
    monkeypatch.setattr(
        "ifer_tool.insee_mod._download_file",
        lambda url, destination_dir: destination_dir / Path(url).name,
    )

    artifact = resolve_uu2020_file(output_dir=tmp_path, target_year=2025)

    assert artifact.year == 2025
    assert artifact.url.endswith("uu2020_2025.csv")
    assert len(calls) >= 2


def test_resolve_cog_file_prefers_direct_information_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "ifer_tool.insee_mod._extract_stat_file_links_from_info_page",
        lambda page_url: [
            "https://www.insee.fr/fr/statistiques/fichier/2521852/cog2024.xls",
            "https://www.insee.fr/fr/statistiques/fichier/2521852/cog2025.xls",
        ],
    )
    monkeypatch.setattr(
        "ifer_tool.insee_mod._download_file",
        lambda url, destination_dir: destination_dir / Path(url).name,
    )

    artifact = resolve_cog_file(output_dir=tmp_path, target_year=2025)

    assert artifact.year == 2025
    assert artifact.url.endswith("cog2025.xls")


def test_resolve_uu2020_file_prefers_direct_information_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "ifer_tool.insee_mod._extract_stat_file_links_from_info_page",
        lambda page_url: [
            "https://www.insee.fr/fr/statistiques/fichier/2531265/base_tu_2020_2025.csv",
            "https://www.insee.fr/fr/statistiques/fichier/2531266/base_td_2020_2024.csv",
        ],
    )
    monkeypatch.setattr(
        "ifer_tool.insee_mod._download_file",
        lambda url, destination_dir: destination_dir / Path(url).name,
    )

    artifact = resolve_uu2020_file(output_dir=tmp_path, target_year=2025)

    assert artifact.year == 2025
    assert artifact.url.endswith("base_tu_2020_2025.csv")


def test_resolve_history_file_uses_local_fallback_when_no_links(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ifer_tool.insee_mod._probe_known_history_links", lambda target_year: [])
    monkeypatch.setattr("ifer_tool.insee_mod._collect_candidate_links", lambda query: [])

    artifact = resolve_history_file(output_dir=tmp_path)

    assert artifact.url.startswith("local-fallback://")
    assert artifact.local_path.exists()
    assert artifact.local_path.read_text(encoding="utf-8") == "old_code,new_code\n"


def test_resolve_history_file_prefers_known_history_links(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "ifer_tool.insee_mod._probe_known_history_links",
        lambda target_year: [
            "https://www.insee.fr/fr/statistiques/fichier/8377162/v_mvt_commune_2025.csv"
        ],
    )
    monkeypatch.setattr(
        "ifer_tool.insee_mod._download_file",
        lambda url, destination_dir: destination_dir / Path(url).name,
    )

    artifact = resolve_history_file(output_dir=tmp_path)

    assert artifact.url.endswith("v_mvt_commune_2025.csv")


def test_prepare_tabular_file_extracts_csv_from_zip(tmp_path: Path) -> None:
    archive_path = tmp_path / "fonds_uu2020_2025.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("README.txt", "metadata")
        archive.writestr("data/base_tu_2020_2025.csv", "code;tuu\n01001;A\n")

    prepared = _prepare_tabular_file(archive_path)

    assert prepared.suffix.lower() == ".csv"
    assert prepared.exists()


def test_prepare_tabular_file_extracts_csv_from_nested_zip(tmp_path: Path) -> None:
    archive_path = tmp_path / "fonds_uu2020_2025.zip"
    inner_buffer = BytesIO()
    with zipfile.ZipFile(inner_buffer, "w") as inner_archive:
        inner_archive.writestr("base_tu_2020_2025.csv", "code;tuu\n01001;A\n")

    with zipfile.ZipFile(archive_path, "w") as outer_archive:
        outer_archive.writestr("sub/com_uu2020_2025.zip", inner_buffer.getvalue())

    prepared = _prepare_tabular_file(archive_path)

    assert prepared.suffix.lower() == ".csv"
    assert prepared.exists()


def test_resolve_cog_file_uses_fichier_endpoint_probe_when_info_page_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ifer_tool.insee_mod._extract_stat_file_links_from_info_page", lambda page_url: [])
    monkeypatch.setattr(
        "ifer_tool.insee_mod._probe_fichier_endpoints",
        lambda product_ids: ["https://www.insee.fr/fr/statistiques/fichier/2560452/cog2024.xls"],
    )
    monkeypatch.setattr(
        "ifer_tool.insee_mod._download_file",
        lambda url, destination_dir: destination_dir / Path(url).name,
    )

    artifact = resolve_cog_file(output_dir=tmp_path, target_year=2025)

    assert artifact.year == 2024
    assert artifact.url.endswith("cog2024.xls")


def test_resolve_cog_file_uses_known_url_probe_when_other_paths_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ifer_tool.insee_mod._extract_stat_file_links_from_info_page", lambda page_url: [])
    monkeypatch.setattr("ifer_tool.insee_mod._probe_fichier_endpoints", lambda product_ids: [])
    monkeypatch.setattr(
        "ifer_tool.insee_mod._probe_known_cog_links",
        lambda target_year: ["https://www.insee.fr/fr/statistiques/fichier/8377162/v_commune_2024.csv"],
    )
    monkeypatch.setattr(
        "ifer_tool.insee_mod._download_file",
        lambda url, destination_dir: destination_dir / Path(url).name,
    )

    artifact = resolve_cog_file(output_dir=tmp_path, target_year=2025)

    assert artifact.year == 2024
    assert artifact.url.endswith("v_commune_2024.csv")


def test_resolve_uu2020_file_uses_fichier_endpoint_probe_when_info_page_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ifer_tool.insee_mod._extract_stat_file_links_from_info_page", lambda page_url: [])
    monkeypatch.setattr(
        "ifer_tool.insee_mod._probe_fichier_endpoints",
        lambda product_ids: ["https://www.insee.fr/fr/statistiques/fichier/4802589/fonds_uu2020_2025.zip"],
    )
    monkeypatch.setattr(
        "ifer_tool.insee_mod._download_file",
        lambda url, destination_dir: destination_dir / Path(url).name,
    )

    artifact = resolve_uu2020_file(output_dir=tmp_path, target_year=2025)

    assert artifact.year == 2025
    assert artifact.url.endswith("fonds_uu2020_2025.zip")


def test_fetch_page_supports_gzip_encoded_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeHeaders:
        def __init__(self) -> None:
            self._headers = {
                "Content-Encoding": "gzip",
                "Content-Type": "application/xml",
            }

        def get(self, key: str) -> str | None:
            return self._headers.get(key)

    class FakeResponse:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload
            self.headers = FakeHeaders()

        def read(self) -> bytes:
            return self._payload

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    payload = gzip.compress(b"<urlset><loc>https://www.insee.fr/fr/statistiques/123</loc></urlset>")
    monkeypatch.setattr("ifer_tool.insee_mod.urlopen", lambda request: FakeResponse(payload))

    from ifer_tool.insee_mod import _fetch_page

    text = _fetch_page("https://www.insee.fr/sitemap.xml.gz")
    assert "<loc>https://www.insee.fr/fr/statistiques/123</loc>" in text


def test_build_insee_duckdb_table_checks_expected_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_file = tmp_path / "file.csv"
    fake_file.write_text("x\n1\n", encoding="utf-8")

    artifact = InseeArtifact(year=2025, url="https://example.test/file.csv", local_path=fake_file)
    monkeypatch.setattr("ifer_tool.insee_mod.resolve_cog_file", lambda output_dir, target_year: artifact)
    monkeypatch.setattr("ifer_tool.insee_mod.resolve_uu2020_file", lambda output_dir, target_year: artifact)
    monkeypatch.setattr("ifer_tool.insee_mod.resolve_history_file", lambda output_dir: artifact)
    monkeypatch.setattr(
        "ifer_tool.insee_mod._build_cog_tuu_tduu_table",
        lambda connection, cog_path, uu_path, history_path, target_year, metro_only: (
            "insee.cog_tuu_tduu_2025",
            10,
        ),
    )

    with pytest.raises(InseeError, match=r"attendu 39071 \+/- 0, obtenu 10"):
        build_insee_duckdb_table(
            insee_dir=tmp_path / "insee",
            database_path=tmp_path / "insee.duckdb",
            target_year=2025,
            expected_rows=39071,
        )


def test_build_insee_duckdb_table_accepts_expected_tolerance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_file = tmp_path / "file.csv"
    fake_file.write_text("x\n1\n", encoding="utf-8")

    artifact = InseeArtifact(year=2025, url="https://example.test/file.csv", local_path=fake_file)
    monkeypatch.setattr("ifer_tool.insee_mod.resolve_cog_file", lambda output_dir, target_year: artifact)
    monkeypatch.setattr("ifer_tool.insee_mod.resolve_uu2020_file", lambda output_dir, target_year: artifact)
    monkeypatch.setattr("ifer_tool.insee_mod.resolve_history_file", lambda output_dir: artifact)
    monkeypatch.setattr(
        "ifer_tool.insee_mod._build_cog_tuu_tduu_table",
        lambda connection, cog_path, uu_path, history_path, target_year, metro_only: (
            "insee.cog_tuu_tduu_2025",
            39074,
        ),
    )

    result = build_insee_duckdb_table(
        insee_dir=tmp_path / "insee",
        database_path=tmp_path / "insee.duckdb",
        target_year=2025,
        expected_rows=39071,
        expected_tolerance=4,
    )

    assert result.row_count == 39074


def test_build_insee_duckdb_table_normalizes_download_dir_to_insee(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_file = tmp_path / "artifact.csv"
    fake_file.write_text("x\n1\n", encoding="utf-8")
    artifact = InseeArtifact(year=2025, url="https://example.test/file.csv", local_path=fake_file)

    observed_dirs: list[Path] = []

    def fake_resolve_cog(output_dir: Path, target_year: int) -> InseeArtifact:
        observed_dirs.append(output_dir)
        return artifact

    monkeypatch.setattr("ifer_tool.insee_mod.resolve_cog_file", fake_resolve_cog)
    monkeypatch.setattr(
        "ifer_tool.insee_mod.resolve_uu2020_file",
        lambda output_dir, target_year: artifact,
    )
    monkeypatch.setattr("ifer_tool.insee_mod.resolve_history_file", lambda output_dir: artifact)
    monkeypatch.setattr(
        "ifer_tool.insee_mod._build_cog_tuu_tduu_table",
        lambda connection, cog_path, uu_path, history_path, target_year, metro_only: (
            "insee.cog_tuu_tduu_2025",
            1,
        ),
    )

    build_insee_duckdb_table(
        insee_dir=tmp_path / "stage",
        database_path=tmp_path / "insee.duckdb",
        target_year=2025,
    )

    assert observed_dirs
    assert observed_dirs[0].name == "insee"


def test_load_anfr_archives_to_duckdb_loads_csv_tables(tmp_path: Path) -> None:
    source_dir = tmp_path / "anfr"
    source_dir.mkdir()
    archive_january = source_dir / "tables-de-reference-janvier-2026.zip"
    archive_february = source_dir / "tables-de-reference-fevrier-2026.zip"

    with zipfile.ZipFile(archive_january, "w") as archive:
        archive.writestr("exports/sites.csv", "id;nom\n1;Alpha\n")
        archive.writestr("exports/supports.csv", "id;hauteur\n10;25\n")

    with zipfile.ZipFile(archive_february, "w") as archive:
        archive.writestr("exports/sites.csv", "id;nom\n2;Beta\n")

    database_path = tmp_path / "anfr.duckdb"
    extract_dir = tmp_path / "extract"

    result = load_anfr_archives_to_duckdb(
        source_dir=source_dir,
        database_path=database_path,
        extract_dir=extract_dir,
        schema="anfr",
    )

    assert result.database_path == database_path
    assert result.loaded_tables == {
        "anfr.exports_sites": 2,
        "anfr.exports_supports": 1,
    }
    assert len(result.extracted_files) == 3

    with duckdb.connect(str(database_path)) as connection:
        connection.execute("SET schema = 'anfr'")
        rows = connection.execute(
            "SELECT id, nom FROM exports_sites ORDER BY id"
        ).fetchall()
        support_rows = connection.execute(
            "SELECT id, hauteur FROM exports_supports"
        ).fetchall()

    assert rows == [("1", "Alpha"), ("2", "Beta")]
    assert support_rows == [("10", "25")]


@pytest.mark.skipif(
    os.getenv("RUN_INSEE_LIVE") != "1",
    reason="Test live INSEE désactivé sans RUN_INSEE_LIVE=1",
)
def test_insee_live_2025_metropole_row_count(tmp_path: Path) -> None:
    result = build_insee_duckdb_table(
        insee_dir=tmp_path / "insee_live",
        database_path=tmp_path / "insee_live.duckdb",
        target_year=2025,
        metro_only=True,
        expected_rows=39071,
        expected_tolerance=4,
    )

    assert abs(result.row_count - 39071) < 5