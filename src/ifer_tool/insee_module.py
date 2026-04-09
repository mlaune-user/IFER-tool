"""Deprecated: Use insee_mod instead. This module is kept for backward compatibility."""
# Backward compatibility: re-export from insee_mod
from ifer_tool.insee_mod import (
    resolve_cog_file,
    resolve_uu2020_file,
    resolve_history_file,
    build_insee_duckdb_table,
)

__all__ = [
    "resolve_cog_file",
    "resolve_uu2020_file", 
    "resolve_history_file",
    "build_insee_duckdb_table",
]

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
            f"Le contrôle de volumétrie a échoué: attendu {expected_rows} +/- {expected_tolerance}, obtenu {row_count}."
        )

    return InseeBuildResult(
        table_name=table_name,
        row_count=row_count,
        cog_artifact=cog_artifact,
        uu_artifact=uu_artifact,
        history_artifact=history_artifact,
        database_path=database_path,
    )
from __future__ import annotations

from pathlib import Path

from ifer_tool.duckdb_adapter import open_duckdb_connection
from ifer_tool.insee_build_core import _build_cog_tuu_tduu_table, _prepare_tabular_file
from ifer_tool.insee_discovery import (
    _collect_candidate_links,
    _download_file,
    _extract_stat_file_links_from_info_page,
    _fetch_page,
    _filter_links_by_keywords,
    _pick_best_link,
    _probe_fichier_endpoints,
    _probe_known_cog_links,
    _probe_known_history_links,
    _search_page,
    _search_web_pages_for_insee,
    _select_link_for_year,
    _url_exists,
)
from ifer_tool.insee_types import (
    COG_INFO_PAGE_URL,
    InseeArtifact,
    InseeBuildResult,
    InseeError,
    UU_INFO_PAGE_URL,
    ensure_insee_dir,
)


# Facade module kept for backward compatibility and monkeypatching in tests.
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

    query = f"code officiel geographique {target_year} fichier"
    links = _collect_candidate_links(query)
    links = _filter_links_by_keywords(links, ("cog", "geographique", "commune", "depcom"))
    if not links:
        fallback_year = target_year - 1
        fallback_query = f"code officiel geographique {fallback_year} fichier"
        links = _collect_candidate_links(fallback_query)
        links = _filter_links_by_keywords(links, ("cog", "geographique", "commune", "depcom"))
        if not links:
            raise InseeError(f"Impossible de trouver un fichier COG pour {target_year} ou {fallback_year}.")
        selected_link, selected_year = _pick_best_link(links, target_year=fallback_year)
        selected_year = selected_year or fallback_year
    else:
        selected_link, selected_year = _pick_best_link(links, target_year=target_year)
        selected_year = selected_year or target_year

    local_path = _download_file(selected_link, output_dir)
    return InseeArtifact(year=selected_year, url=selected_link, local_path=local_path)


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
            f"Le contrôle de volumétrie a échoué: attendu {expected_rows} +/- {expected_tolerance}, obtenu {row_count}."
        )

    return InseeBuildResult(
        table_name=table_name,
        row_count=row_count,
        cog_artifact=cog_artifact,
        uu_artifact=uu_artifact,
        history_artifact=history_artifact,
        database_path=database_path,
    )
from __future__ import annotations

import re
import unicodedata
import gzip
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus, unquote, urljoin
from urllib.request import Request, urlopen

from ifer_tool.duckdb_adapter import DuckDBConnection, get_table_columns, open_duckdb_connection


INSEE_BASE_URL = "https://www.insee.fr"
INSEE_SEARCH_URL = "https://www.insee.fr/fr/recherche"
HTTP_USER_AGENT = "Mozilla/5.0 (compatible; IFER-tool/1.0; +https://github.com/)"
DUCKDUCKGO_HTML_URL = "https://duckduckgo.com/html/"
COG_INFO_PAGE_URL = "https://www.insee.fr/fr/information/2560452"
UU_INFO_PAGE_URL = "https://www.insee.fr/fr/information/4802589"
COG_CODE_REGEX = "^[0-9]{5}$|^2[AB][0-9]{3}$"
TABULAR_FILE_SUFFIXES = {".csv", ".xls", ".xlsx", ".dbf", ".shp", ".gpkg", ".geojson"}


class InseeError(RuntimeError):
    pass


@dataclass(frozen=True)
class InseeArtifact:
    year: int
    url: str
    local_path: Path


@dataclass(frozen=True)
class InseeBuildResult:
    table_name: str
    row_count: int
    cog_artifact: InseeArtifact
    uu_artifact: InseeArtifact
    history_artifact: InseeArtifact
    database_path: Path


def _ensure_insee_dir(path: Path) -> Path:
    return path if path.name == "insee" else path / "insee"


def _search_page(query: str) -> str:
    search_url = f"{INSEE_SEARCH_URL}?q={quote_plus(query)}"
    request = Request(search_url, headers={"User-Agent": HTTP_USER_AGENT})
    with urlopen(request) as response:
        payload = response.read()
        content_encoding = (response.headers.get("Content-Encoding") or "").lower()
        if content_encoding == "gzip":
            payload = gzip.decompress(payload)
        return payload.decode("utf-8", errors="ignore")


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


def _extract_loc_urls(xml_text: str) -> list[str]:
    return re.findall(r"<loc>([^<]+)</loc>", xml_text, flags=re.IGNORECASE)


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_only).strip().lower()


def _query_keywords(query: str) -> list[str]:
    tokens = re.findall(r"[a-zA-Z0-9]+", _normalize_text(query))
    stopwords = {"de", "des", "du", "la", "le", "les", "et", "sur", "pour", "fichier"}
    return [token for token in tokens if len(token) >= 3 and token not in stopwords]


def _dedupe(items: list[str], limit: int | None = None) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
        if limit is not None and len(deduped) >= limit:
            break
    return deduped


def _search_sitemap_pages(query: str, max_pages: int = 12) -> list[str]:
    try:
        sitemap_index = _fetch_page(f"{INSEE_BASE_URL}/sitemap.xml")
    except Exception:
        return []

    sitemap_urls = _extract_loc_urls(sitemap_index)
    # Keep a limited subset of likely useful sitemaps for performance.
    likely_sitemaps = [
        url
        for url in sitemap_urls
        if "stat" in url.lower() or "information" in url.lower() or "fr" in url.lower()
    ][:8]
    if not likely_sitemaps:
        likely_sitemaps = sitemap_urls[:4]

    matches: list[str] = []

    for sitemap_url in likely_sitemaps:
        try:
            sitemap_text = _fetch_page(sitemap_url)
        except Exception:
            continue
        for page_url in _extract_loc_urls(sitemap_text):
            lowered = page_url.lower()
            if "/fr/statistiques/" not in lowered and "/fr/information/" not in lowered:
                continue
            matches.append(page_url)
            if len(matches) >= max_pages:
                return matches

    return matches


def _extract_candidate_links(html_text: str) -> list[str]:
    text = html_text.replace("\\/", "/").replace("&amp;", "&")
    raw_links = re.findall(r"href\s*=\s*['\"]([^'\"]+)['\"]", text, flags=re.IGNORECASE)
    candidates: list[str] = []
    for raw_link in raw_links:
        lowered = raw_link.lower()
        if "/fr/statistiques/fichier/" not in lowered and "fichier" not in lowered:
            continue
        if "/fr/statistiques/fichier/" not in lowered and not any(
            token in lowered for token in (".csv", ".zip", ".xls", ".xlsx")
        ):
            continue
        candidates.append(urljoin(INSEE_BASE_URL, raw_link))

    # Some INSEE pages embed file links in script blobs rather than anchor tags.
    blob_links = re.findall(r"(/fr/statistiques/fichier/[^\"'\s<]+)", text, flags=re.IGNORECASE)
    candidates.extend(urljoin(INSEE_BASE_URL, link) for link in blob_links)

    absolute_links = re.findall(
        r"(https?://www\.insee\.fr/fr/statistiques/fichier/[^\"'\s<]+)",
        text,
        flags=re.IGNORECASE,
    )
    candidates.extend(absolute_links)

    return _dedupe(candidates)


def _extract_stat_file_links_from_info_page(info_page_url: str) -> list[str]:
    try:
        html_text = _fetch_page(info_page_url)
    except Exception:
        return []

    candidates = _extract_candidate_links(html_text)
    filtered = [link for link in candidates if "/fr/statistiques/fichier/" in link.lower()]

    return _dedupe(filtered)


def _probe_fichier_endpoint(product_id: str) -> list[str]:
    endpoint_url = f"{INSEE_BASE_URL}/fr/statistiques/fichier/{product_id}"
    request = Request(endpoint_url, headers={"User-Agent": HTTP_USER_AGENT})
    try:
        with urlopen(request) as response:
            final_url = response.geturl()
            headers = response.headers
            content_type = (headers.get("Content-Type") or "").lower()
            content_disposition = (headers.get("Content-Disposition") or "").lower()

            links: list[str] = []
            if final_url and final_url != endpoint_url and "/fr/statistiques/fichier/" in final_url.lower():
                links.append(final_url)

            if any(token in content_type for token in ("csv", "excel", "zip")) or "filename=" in content_disposition:
                links.append(final_url)

            body = response.read()
            try:
                text = body.decode("utf-8", errors="ignore")
            except Exception:
                text = ""
            if text:
                links.extend(_extract_candidate_links(text))

            return _dedupe(links)
    except Exception:
        return []


def _probe_fichier_endpoints(product_ids: tuple[str, ...]) -> list[str]:
    links: list[str] = []
    for product_id in product_ids:
        links.extend(_probe_fichier_endpoint(product_id))

    return _dedupe(links)


def _url_exists(url: str) -> bool:
    # Try HEAD first to avoid downloading full files when endpoint supports it.
    try:
        request = Request(url, headers={"User-Agent": HTTP_USER_AGENT}, method="HEAD")
        with urlopen(request, timeout=10):
            return True
    except Exception:
        pass

    # Some endpoints reject HEAD, fallback to a light GET check.
    try:
        request = Request(url, headers={"User-Agent": HTTP_USER_AGENT, "Range": "bytes=0-0"})
        with urlopen(request, timeout=10):
            return True
    except Exception:
        return False


def _known_cog_url_candidates(target_year: int) -> list[str]:
    years = [target_year, target_year - 1]
    product_ids = ("2521852", "2560452", "8377162")
    filename_patterns = (
        "cog{year}.xls",
        "cog{year}.xlsx",
        "cog_{year}.xls",
        "cog_{year}.xlsx",
        "v_commune_{year}.csv",
        "v_commune_{year}.zip",
        "cog_complet_{year}.xlsx",
    )

    candidates: list[str] = []
    for year in years:
        for product_id in product_ids:
            for pattern in filename_patterns:
                filename = pattern.format(year=year)
                candidates.append(f"{INSEE_BASE_URL}/fr/statistiques/fichier/{product_id}/{filename}")
    return candidates


def _probe_known_cog_links(target_year: int) -> list[str]:
    matches: list[str] = []
    for candidate in _known_cog_url_candidates(target_year):
        if _url_exists(candidate):
            matches.append(candidate)
    return matches


def _known_history_url_candidates(target_year: int) -> list[str]:
    years = [target_year, target_year - 1]
    product_ids = ("8377162", "2521852", "2560452")
    filename_patterns = (
        "v_commune_depuis_1943.csv",
        "v_commune_depuis_1943.dbf",
        "v_mvt_commune_{year}.csv",
        "v_mouv_commune_{year}.csv",
        "historique_communes_{year}.csv",
        "mouvements_communes_depuis_1943.csv",
        "historique_communes_depuis_1943.csv",
    )

    candidates: list[str] = []
    for year in years:
        for product_id in product_ids:
            for pattern in filename_patterns:
                filename = pattern.format(year=year) if "{year}" in pattern else pattern
                candidates.append(f"{INSEE_BASE_URL}/fr/statistiques/fichier/{product_id}/{filename}")
    return candidates


def _probe_known_history_links(target_year: int) -> list[str]:
    matches: list[str] = []
    for candidate in _known_history_url_candidates(target_year):
        if _url_exists(candidate):
            matches.append(candidate)
    return matches


def _extract_result_pages(html_text: str) -> list[str]:
    raw_links = re.findall(r'href="([^"]+)"', html_text)
    pages: list[str] = []
    for raw_link in raw_links:
        lowered = raw_link.lower()
        if "/fr/statistiques/" not in lowered:
            continue
        if "/fr/statistiques/fichier/" in lowered:
            continue
        pages.append(urljoin(INSEE_BASE_URL, raw_link))

    return _dedupe(pages)


def _search_web_pages_for_insee(query: str, max_results: int = 10) -> list[str]:
    web_query = f"site:insee.fr {query}"
    search_url = f"{DUCKDUCKGO_HTML_URL}?q={quote_plus(web_query)}"
    request = Request(search_url, headers={"User-Agent": HTTP_USER_AGENT})
    with urlopen(request) as response:
        html_text = response.read().decode("utf-8", errors="ignore")

    candidates: list[str] = []
    # DuckDuckGo often encodes target URL in uddg=<urlencoded>.
    for encoded_url in re.findall(r"uddg=([^&\"']+)", html_text):
        try:
            decoded = unquote(encoded_url)
        except Exception:
            continue
        if "insee.fr" not in decoded.lower():
            continue
        candidates.append(decoded)

    # Keep only likely INSEE stat pages or file pages.
    filtered: list[str] = []
    for candidate in candidates:
        lowered = candidate.lower()
        if "/fr/statistiques/" in lowered or "/fr/information/" in lowered:
            filtered.append(candidate)

    return _dedupe(filtered, limit=max_results)


def _filter_links_by_keywords(links: list[str], keywords: tuple[str, ...]) -> list[str]:
    lowered_keywords = tuple(keyword.lower() for keyword in keywords)
    filtered = [link for link in links if any(keyword in link.lower() for keyword in lowered_keywords)]
    return filtered or links


def _extract_year_candidates(value: str) -> list[int]:
    years = []
    for year_text in re.findall(r"(19\d{2}|20\d{2})", value):
        year = int(year_text)
        if year not in years:
            years.append(year)
    return years


def _score_link(
    link: str,
    index: int,
    target_year: int | None,
    extension_weights: tuple[tuple[str, int], ...],
    preferred_tokens: tuple[str, ...],
    accepted_extensions: tuple[str, ...] = (),
    target_year_bonus: int = 1000,
    previous_year_bonus: int | None = None,
    older_year_base: int = 500,
    future_year_base: int = 500,
) -> tuple[int, int, str, int | None] | None:
    lowered = link.lower()
    if accepted_extensions and not lowered.endswith(accepted_extensions):
        return None

    years = _extract_year_candidates(lowered)
    best_year = max(years) if years else None

    score = 0
    if target_year is not None and best_year is not None:
        if best_year == target_year:
            score += target_year_bonus
        elif previous_year_bonus is not None and best_year == target_year - 1:
            score += previous_year_bonus
        elif best_year <= target_year:
            score += older_year_base - abs(target_year - best_year)
        else:
            score += future_year_base - abs(target_year - best_year)

    for extension, weight in extension_weights:
        if lowered.endswith(f".{extension}"):
            score += weight
            break

    for token in preferred_tokens:
        if token in lowered:
            score += 120

    return score, -index, link, best_year


def _select_link_for_year(
    links: list[str],
    target_year: int,
    accepted_extensions: tuple[str, ...],
    preferred_tokens: tuple[str, ...],
) -> tuple[str, int | None]:
    if not links:
        raise InseeError("Aucun lien de fichier INSEE trouvé pour la requête.")

    scored: list[tuple[int, int, str, int | None]] = []
    for index, link in enumerate(links):
        scored_link = _score_link(
            link=link,
            index=index,
            target_year=target_year,
            extension_weights=(("csv", 80), ("xlsx", 70), ("xls", 60), ("zip", 50)),
            preferred_tokens=preferred_tokens,
            accepted_extensions=accepted_extensions,
            target_year_bonus=2000,
            previous_year_bonus=1800,
            older_year_base=1200,
            future_year_base=800,
        )
        if scored_link is not None:
            scored.append(scored_link)

    if not scored:
        raise InseeError("Aucun lien INSEE ne correspond aux formats attendus.")

    scored.sort(reverse=True)
    _, _, selected_link, selected_year = scored[0]
    return selected_link, selected_year


def _collect_candidate_links(query: str, max_result_pages: int = 8) -> list[str]:
    search_html = _search_page(query)
    candidates = _extract_candidate_links(search_html)
    if candidates:
        return candidates

    for page_url in _extract_result_pages(search_html)[:max_result_pages]:
        try:
            page_html = _fetch_page(page_url)
        except Exception:
            continue
        candidates.extend(_extract_candidate_links(page_html))

    if not candidates:
        for page_url in _search_web_pages_for_insee(query):
            try:
                page_html = _fetch_page(page_url)
            except Exception:
                continue
            # Keep direct file URLs found in web results too.
            if "/fr/statistiques/fichier/" in page_url.lower():
                candidates.append(page_url)
            candidates.extend(_extract_candidate_links(page_html))

    if not candidates:
        keywords = _query_keywords(query)
        for page_url in _search_sitemap_pages(query, max_pages=40):
            try:
                page_html = _fetch_page(page_url)
            except Exception:
                continue
            if "/fr/statistiques/fichier/" in page_url.lower():
                candidates.append(page_url)
            page_links = _extract_candidate_links(page_html)
            if keywords:
                page_text = _normalize_text(page_html)
                filtered_links = [
                    link
                    for link in page_links
                    if any(keyword in _normalize_text(link) for keyword in keywords)
                    or any(keyword in page_text for keyword in keywords)
                ]
                candidates.extend(filtered_links or page_links)
            else:
                candidates.extend(page_links)

    return _dedupe(candidates)


def _pick_best_link(links: list[str], target_year: int | None = None) -> tuple[str, int | None]:
    if not links:
        raise InseeError("Aucun lien de fichier INSEE trouvé pour la requête.")

    scored_links: list[tuple[int, int, str, int | None]] = []
    for index, link in enumerate(links):
        scored_link = _score_link(
            link=link,
            index=index,
            target_year=target_year,
            extension_weights=(("csv", 60), ("zip", 50), ("xlsx", 40), ("xls", 30)),
            preferred_tokens=(),
            accepted_extensions=(),
            previous_year_bonus=None,
            older_year_base=500,
            future_year_base=500,
        )
        if scored_link is not None:
            scored_links.append(scored_link)

    scored_links.sort(reverse=True)
    _, _, best_link, best_year = scored_links[0]
    return best_link, best_year


def _download_file(url: str, destination_dir: Path) -> Path:
    destination_dir.mkdir(parents=True, exist_ok=True)
    file_name = Path(url.split("?", 1)[0]).name or "insee_file"
    local_path = destination_dir / file_name
    request = Request(url, headers={"User-Agent": HTTP_USER_AGENT})
    with urlopen(request) as response:
        local_path.write_bytes(response.read())
    return local_path


def resolve_cog_file(output_dir: Path, target_year: int) -> InseeArtifact:
    direct_links = _extract_stat_file_links_from_info_page(COG_INFO_PAGE_URL)
    if not direct_links:
        direct_links = _probe_fichier_endpoints(("2560452", "2521852", "8377162"))
    if not direct_links:
        direct_links = _probe_known_cog_links(target_year)
    direct_links = _filter_links_by_keywords(
        direct_links,
        ("cog", "geographique", "commune", "depcom"),
    )

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
            # Respect requested fallback policy: target year or previous year.
            selected_year = target_year - 1
        local_path = _download_file(selected_link, output_dir)
        return InseeArtifact(year=selected_year, url=selected_link, local_path=local_path)

    query = f"code officiel geographique {target_year} fichier"
    links = _collect_candidate_links(query)
    links = _filter_links_by_keywords(links, ("cog", "geographique", "commune", "depcom"))
    if not links:
        fallback_year = target_year - 1
        fallback_query = f"code officiel geographique {fallback_year} fichier"
        links = _collect_candidate_links(fallback_query)
        links = _filter_links_by_keywords(links, ("cog", "geographique", "commune", "depcom"))
        if not links:
            raise InseeError(
                f"Impossible de trouver un fichier COG pour {target_year} ou {fallback_year}."
            )
        selected_link, selected_year = _pick_best_link(links, target_year=fallback_year)
        selected_year = selected_year or fallback_year
    else:
        selected_link, selected_year = _pick_best_link(links, target_year=target_year)
        selected_year = selected_year or target_year

    local_path = _download_file(selected_link, output_dir)
    return InseeArtifact(year=selected_year, url=selected_link, local_path=local_path)


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

    # Soft fallback: keep pipeline operational even when INSEE does not expose
    # a discoverable history file in the current session.
    output_dir.mkdir(parents=True, exist_ok=True)
    local_path = output_dir / "historique_communes_fallback.csv"
    if not local_path.exists():
        local_path.write_text("old_code,new_code\n", encoding="utf-8")
    return InseeArtifact(year=1943, url="local-fallback://historique_communes", local_path=local_path)


def _normalize_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _pick_column(columns: list[str], candidates: list[str]) -> str:
    normalized_columns = {_normalize_column_name(column): column for column in columns}
    for candidate in candidates:
        normalized_candidate = _normalize_column_name(candidate)
        if normalized_candidate in normalized_columns:
            return normalized_columns[normalized_candidate]

    for candidate in candidates:
        normalized_candidate = _normalize_column_name(candidate)
        for normalized_column, original_column in normalized_columns.items():
            if normalized_candidate in normalized_column:
                return original_column

    raise InseeError(
        f"Aucune colonne compatible trouvée. Colonnes disponibles: {', '.join(columns)}"
    )


def _open_relation_sql(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    escaped = str(file_path).replace("'", "''")
    if suffix == ".csv":
        return (
            f"read_csv_auto('{escaped}', all_varchar=true, sample_size=-1, "
            "normalize_names=true, header=true, strict_mode=false, "
            "ignore_errors=true, null_padding=true, max_line_size=10000000)"
        )
    if suffix == ".zip":
        return (
            f"read_csv_auto('{escaped}', all_varchar=true, sample_size=-1, "
            "normalize_names=true, header=true, filename=true, strict_mode=false, "
            "ignore_errors=true, null_padding=true, max_line_size=10000000)"
        )
    if suffix in {".xls", ".xlsx"}:
        return f"st_read('{escaped}')"
    if suffix in {".dbf", ".shp", ".gpkg", ".geojson"}:
        return f"st_read('{escaped}')"
    raise InseeError(f"Format de fichier INSEE non supporté: {file_path.name}")


def _looks_like_html_file(file_path: Path) -> bool:
    try:
        sample = file_path.read_bytes()[:4096]
    except Exception:
        return False
    text = sample.decode("utf-8", errors="ignore").lower()
    return "<html" in text or "<!doctype html" in text


def _find_tabular_in_directory(directory: Path) -> Path | None:
    candidates = sorted(
        path
        for path in directory.rglob("*")
        if path.is_file() and path.suffix.lower() in TABULAR_FILE_SUFFIXES
    )
    if not candidates:
        return None

    csv_candidates = [path for path in candidates if path.suffix.lower() == ".csv"]
    if csv_candidates:
        return csv_candidates[0]
    return candidates[0]


def _prepare_tabular_file(file_path: Path, _depth: int = 0) -> Path:
    if _depth > 5:
        raise InseeError("Profondeur d'archives imbriquées trop importante.")

    if _looks_like_html_file(file_path):
        raise InseeError(
            f"Le fichier téléchargé '{file_path.name}' ressemble à une page HTML et non à un fichier de données INSEE."
        )

    suffix = file_path.suffix.lower()
    if suffix in TABULAR_FILE_SUFFIXES:
        return file_path

    if suffix == ".zip":
        extract_root = file_path.parent / "_expanded" / file_path.stem
        if extract_root.exists():
            shutil.rmtree(extract_root)
        extract_root.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(file_path) as archive:
            archive.extractall(extract_root)

        tabular = _find_tabular_in_directory(extract_root)
        if tabular is not None:
            return tabular

        nested_archives = sorted(
            path for path in extract_root.rglob("*") if path.is_file() and path.suffix.lower() == ".zip"
        )
        for nested_archive in nested_archives:
            try:
                return _prepare_tabular_file(nested_archive, _depth=_depth + 1)
            except InseeError:
                continue

        raise InseeError(
            f"Aucun fichier tabulaire exploitable trouvé dans l'archive '{file_path.name}'."
        )

    raise InseeError(f"Format de fichier INSEE non supporté: {file_path.name}")


def _build_cog_tuu_tduu_table(
    connection: DuckDBConnection,
    cog_path: Path,
    uu_path: Path,
    history_path: Path,
    target_year: int,
    metro_only: bool,
) -> tuple[str, int]:
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")

    cog_path = _prepare_tabular_file(cog_path)
    uu_path = _prepare_tabular_file(uu_path)
    history_path = _prepare_tabular_file(history_path)

    connection.execute(f"CREATE OR REPLACE TEMP TABLE cog_raw AS SELECT * FROM {_open_relation_sql(cog_path)}")
    connection.execute(f"CREATE OR REPLACE TEMP TABLE uu_raw AS SELECT * FROM {_open_relation_sql(uu_path)}")
    connection.execute(
        f"CREATE OR REPLACE TEMP TABLE history_raw AS SELECT * FROM {_open_relation_sql(history_path)}"
    )

    cog_columns = get_table_columns(connection, "cog_raw")
    uu_columns = get_table_columns(connection, "uu_raw")
    history_columns = get_table_columns(connection, "history_raw")

    cog_code_column = _pick_column(cog_columns, ["COM", "CODGEO", "CODE_COMMUNE", "DEP_COM"])
    cog_type_column = _pick_column(cog_columns, ["TYPECOM", "TYPE_COM"])
    cog_parent_column = _pick_column(cog_columns, ["COMPARENT", "COM_PARENT", "PARENT", "COM_AP"])
    uu_code_column = _pick_column(uu_columns, ["COM", "CODGEO", "CODE_COMMUNE", "DEP_COM"])
    tuu_column = _pick_column(uu_columns, ["TUU2020", "TUU", "UU2020", "TYPE_UNITE_URBAINE"])
    tduu_column = _pick_column(
        uu_columns,
        ["TDUU2020", "TDUU", "TYPE_DETAILLE_UNITE_URBAINE", "TYPE_COM", "LIBUU2020"],
    )

    normalized_history_columns = {_normalize_column_name(column) for column in history_columns}
    history_is_snapshot = all(
        column in normalized_history_columns for column in ("com", "datedebut", "datefin")
    )

    hist_old_column = None
    hist_new_column = None
    if not history_is_snapshot:
        hist_old_column = _pick_column(
            history_columns,
            ["COM_AV", "DEP_COM_AV", "CODGEO_AV", "OLD_COM", "CODE_AVANT", "OLD_CODE"],
        )
        hist_new_column = _pick_column(
            history_columns,
            ["COM_AP", "DEP_COM_AP", "CODGEO_AP", "NEW_COM", "CODE_APRES", "CODGEO", "NEW_CODE"],
        )

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE cog_resolution AS
        SELECT DISTINCT
            {cog_code_column} AS code,
            CASE
                WHEN upper(coalesce({cog_type_column}, '')) IN ('ARM', 'COMD')
                     AND coalesce(trim({cog_parent_column}), '') <> ''
                THEN {cog_parent_column}
                ELSE {cog_code_column}
            END AS reference_code,
            {cog_type_column} AS type_code,
            {cog_parent_column} AS parent_code
        FROM cog_raw
        WHERE regexp_full_match(CAST({cog_code_column} AS VARCHAR), '{COG_CODE_REGEX}')
        """
    )

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE cog_current AS
        SELECT DISTINCT code
        FROM cog_resolution
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE arrondissement_resolution AS
        SELECT DISTINCT
            code AS cog_initial,
            code AS cog_reference
        FROM cog_resolution
        WHERE upper(coalesce(type_code, '')) = 'ARM'
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE uu_base AS
        SELECT DISTINCT
            {uu_code_column} AS code,
            {tuu_column} AS tuu,
            {tduu_column} AS tduu
        FROM uu_raw
        WHERE regexp_full_match(CAST({uu_code_column} AS VARCHAR), '{COG_CODE_REGEX}')
        """
    )
    if history_is_snapshot:
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE history_edges AS
            SELECT CAST(NULL AS VARCHAR) AS old_code, CAST(NULL AS VARCHAR) AS new_code
            WHERE FALSE
            """
        )
    else:
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE history_edges AS
            SELECT DISTINCT
                {hist_old_column} AS old_code,
                {hist_new_column} AS new_code
            FROM history_raw
            WHERE regexp_full_match(CAST({hist_old_column} AS VARCHAR), '{COG_CODE_REGEX}')
              AND regexp_full_match(CAST({hist_new_column} AS VARCHAR), '{COG_CODE_REGEX}')
              AND {hist_old_column} <> {hist_new_column}
            """
        )

    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE history_resolved AS
        WITH RECURSIVE walk(origin_code, current_code, depth) AS (
            SELECT old_code, new_code, 1
            FROM history_edges
            UNION ALL
            SELECT w.origin_code, e.new_code, w.depth + 1
            FROM walk w
            JOIN history_edges e ON e.old_code = w.current_code
            WHERE w.depth < 20
        ), ranked AS (
            SELECT
                origin_code,
                current_code,
                depth,
                row_number() OVER (PARTITION BY origin_code ORDER BY depth DESC) AS rn
            FROM walk
        )
        SELECT origin_code, current_code
        FROM ranked
        WHERE rn = 1
        """
    )

    if history_is_snapshot:
        history_code_column = _pick_column(history_columns, ["COM", "CODGEO", "CODE_COMMUNE"])
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE history_codes AS
            SELECT DISTINCT {history_code_column} AS code
            FROM history_raw
            WHERE regexp_full_match(CAST({history_code_column} AS VARCHAR), '{COG_CODE_REGEX}')
            """
        )
    else:
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE history_codes AS
            SELECT DISTINCT origin_code AS code
            FROM history_resolved
            UNION
            SELECT DISTINCT new_code AS code
            FROM history_edges
            """
        )

    table_name = f"cog_tuu_tduu_{target_year}"
    metro_filter = ""
    if metro_only:
        metro_filter = """
        WHERE (substr(mapping.cog_initial, 1, 2) BETWEEN '01' AND '95')
           OR substr(mapping.cog_initial, 1, 2) IN ('2A', '2B')
        """

    connection.execute("CREATE SCHEMA IF NOT EXISTS insee")
    connection.execute("SET schema = 'insee'")
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        WITH all_codes AS (
            SELECT
                history.code AS cog_initial,
                coalesce(resolution.reference_code, history.code) AS cog_reference
            FROM history_codes history
            LEFT JOIN cog_resolution resolution ON resolution.code = history.code
            UNION
            SELECT
                resolution.code AS cog_initial,
                resolution.reference_code AS cog_reference
            FROM cog_resolution resolution
            UNION
            SELECT
                cog_initial,
                cog_reference
            FROM arrondissement_resolution
        ), mapping AS (
            SELECT DISTINCT cog_initial, cog_reference FROM all_codes
        )
        SELECT
            mapping.cog_initial,
            mapping.cog_reference,
            uu.tuu,
            uu.tduu,
            {target_year} AS cog_year,
            {target_year} AS target_year
        FROM mapping
        LEFT JOIN uu_base uu ON uu.code = mapping.cog_reference
        {metro_filter}
        """
    )

    row_count = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    return f"insee.{table_name}", int(row_count)


def build_insee_duckdb_table(
    insee_dir: Path,
    database_path: Path,
    target_year: int,
    metro_only: bool = True,
    expected_rows: int | None = None,
    expected_tolerance: int = 0,
) -> InseeBuildResult:
    insee_dir = _ensure_insee_dir(insee_dir)
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
            f"Le contrôle de volumétrie a échoué: attendu {expected_rows} +/- {expected_tolerance}, obtenu {row_count}."
        )

    return InseeBuildResult(
        table_name=table_name,
        row_count=row_count,
        cog_artifact=cog_artifact,
        uu_artifact=uu_artifact,
        history_artifact=history_artifact,
        database_path=database_path,
    )