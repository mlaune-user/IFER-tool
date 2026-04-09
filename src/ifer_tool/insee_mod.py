"""INSEE data resolution and DuckDB integration."""
from __future__ import annotations

import gzip
import re
from pathlib import Path
from urllib.request import Request, urlopen

from ifer_tool.duckdb_adapter import open_duckdb_connection
from ifer_tool.insee_build_core import _build_cog_tuu_tduu_table
from ifer_tool.insee_discovery import (
    _collect_candidate_links,
    _download_file,
    _extract_stat_file_links_from_info_page,
    _fetch_page as _fetch_page_discovery,
    _filter_links_by_keywords,
    _pick_best_link,
    _probe_fichier_endpoints,
    _probe_known_cog_links,
    _probe_known_history_links,
    _search_page,
    _search_web_pages_for_insee,
    _select_link_for_year,
)
from ifer_tool.insee_types import (
    COG_INFO_PAGE_URL,
    HTTP_USER_AGENT,
    INSEE_BASE_URL,
    UU_INFO_PAGE_URL,
    InseeArtifact,
    InseeBuildResult,
    InseeError,
    ensure_insee_dir,
)


def _extract_candidate_links(html_text: str) -> list[str]:
    text = html_text.replace("\\/", "/").replace("&amp;", "&")
    raw_links = re.findall(r"href\s*=\s*['\"]([^'\"]+)['\"]", text, flags=re.IGNORECASE)
    candidates: list[str] = []
    for raw_link in raw_links:
        lowered = raw_link.lower()
        if "/fr/statistiques/fichier/" not in lowered and "fichier" not in lowered:
            continue
        candidates.append(raw_link if raw_link.startswith("http") else f"{INSEE_BASE_URL}{raw_link}")
    return candidates


def _fetch_page(url: str) -> str:
    request = Request(url, headers={"User-Agent": HTTP_USER_AGENT})
    with urlopen(request) as response:
        payload = response.read()
        content_encoding = (response.headers.get("Content-Encoding") or "").lower()
        content_type = (response.headers.get("Content-Type") or "").lower()
        if content_encoding == "gzip" or url.lower().endswith(".gz") or "gzip" in content_type:
            try:
                payload = gzip.decompress(payload)
            except OSError:
                pass
        return payload.decode("utf-8", errors="ignore")


def resolve_cog_file(output_dir: Path, target_year: int) -> InseeArtifact:
    direct_links = _extract_stat_file_links_from_info_page(COG_INFO_PAGE_URL)
    if not direct_links:
        direct_links = _probe_fichier_endpoints(("2560452", "2521852", "8377162"))
    if not direct_links:
        direct_links = _probe_known_cog_links(target_year)
    direct_links = _filter_links_by_keywords(direct_links, ("cog", "geographique", "commune", "depcom"))

    if direct_links:
        selected_link, selected_year = _select_link_for_year(
            direct_links,
            target_year=target_year,
            accepted_extensions=(".xls", ".xlsx", ".csv", ".zip"),
            preferred_tokens=("cog",),
        )
        if selected_year is None:
            selected_year = target_year
        if selected_year > target_year:
            selected_year = target_year
        if selected_year < target_year - 1:
            selected_year = target_year - 1
        local_path = _download_file(selected_link, output_dir)
        return InseeArtifact(year=selected_year, url=selected_link, local_path=local_path)

    for year in (target_year, target_year - 1):
        query = f"code officiel geographique {year} fichier"
        html = _search_page(query)
        links = _extract_candidate_links(html)
        if not links:
            for page_url in _search_web_pages_for_insee(query):
                try:
                    page_html = _fetch_page(page_url)
                except Exception:
                    continue
                links.extend(_extract_candidate_links(page_html))
        links = _filter_links_by_keywords(links, ("cog", "geographique", "commune", "depcom"))
        if links:
            selected_link, selected_year = _pick_best_link(links, target_year=year)
            local_path = _download_file(selected_link, output_dir)
            return InseeArtifact(year=selected_year or year, url=selected_link, local_path=local_path)

    raise InseeError(f"Impossible de trouver un fichier COG pour {target_year} ou {target_year - 1}.")


def resolve_uu2020_file(output_dir: Path, target_year: int) -> InseeArtifact:
    direct_links = _extract_stat_file_links_from_info_page(UU_INFO_PAGE_URL)
    if not direct_links:
        direct_links = _probe_fichier_endpoints(("4802589", "2531265", "2531266"))
    direct_links = _filter_links_by_keywords(
        direct_links,
        ("unite", "urbaine", "tuu", "tduu", "uu2020", "base_tu_2020", "base_td_2020"),
    )
    if direct_links:
        selected_link, selected_year = _select_link_for_year(
            direct_links,
            target_year=target_year,
            accepted_extensions=(".csv", ".xlsx", ".xls", ".zip"),
            preferred_tokens=("base_tu_2020", "base_td_2020", "tuu", "tduu", "unite", "urbaine"),
        )
        local_path = _download_file(selected_link, output_dir)
        return InseeArtifact(year=selected_year or target_year, url=selected_link, local_path=local_path)

    queries = [
        f"base communale unites urbaines 2020 {target_year} fichier",
        "base communale unites urbaines 2020 fichier",
        "unites urbaines 2020 tuu tduu fichier",
        f"unites urbaines 2020 {target_year}",
    ]
    links: list[str] = []
    for query in queries:
        links = _collect_candidate_links(query)
        links = _filter_links_by_keywords(links, ("unite", "urbaine", "tuu", "tduu", "uu2020"))
        if links:
            break

    selected_link, selected_year = _pick_best_link(links, target_year=target_year)
    local_path = _download_file(selected_link, output_dir)
    return InseeArtifact(year=selected_year or target_year, url=selected_link, local_path=local_path)


def resolve_history_file(output_dir: Path) -> InseeArtifact:
    links = _probe_known_history_links(target_year=2025)
    if not links:
        query = "historique des communes 1943 fichier"
        links = _collect_candidate_links(query)
        links = _filter_links_by_keywords(links, ("historique", "communes", "mouvement", "1943", "mvt"))
    if links:
        selected_link, selected_year = _pick_best_link(links, target_year=None)
        local_path = _download_file(selected_link, output_dir)
        return InseeArtifact(year=selected_year or 1943, url=selected_link, local_path=local_path)

    output_dir.mkdir(parents=True, exist_ok=True)
    local_path = output_dir / "historique_communes_fallback.csv"
    if not local_path.exists():
        local_path.write_text("old_code,new_code\n", encoding="utf-8")
    return InseeArtifact(year=1943, url="local-fallback://historique_communes", local_path=local_path)


def build_insee_duckdb_table(
    insee_dir: Path,
    database_path: Path,
    target_year: int,
    metro_only: bool = True,
    expected_rows: int | None = None,
    expected_tolerance: int = 0,
) -> InseeBuildResult:
    insee_dir = ensure_insee_dir(insee_dir)
    insee_dir.mkdir(parents=True, exist_ok=True)

    cog_artifact = resolve_cog_file(output_dir=insee_dir, target_year=target_year)
    uu_artifact = resolve_uu2020_file(output_dir=insee_dir, target_year=target_year)
    history_artifact = resolve_history_file(output_dir=insee_dir)

    with open_duckdb_connection(database_path) as connection:
        table_name, row_count = _build_cog_tuu_tduu_table(
            connection=connection,
            cog_path=cog_artifact.local_path,
            uu_path=uu_artifact.local_path,
            history_path=history_artifact.local_path,
            target_year=target_year,
            metro_only=metro_only,
        )

    if expected_rows is not None and abs(row_count - expected_rows) > expected_tolerance:
        raise InseeError(
            f"Le controle de volumetrie a echoue: attendu {expected_rows} +/- {expected_tolerance}, obtenu {row_count}."
        )

    return InseeBuildResult(
        table_name=table_name,
        row_count=row_count,
        cog_artifact=cog_artifact,
        uu_artifact=uu_artifact,
        history_artifact=history_artifact,
        database_path=database_path,
    )
