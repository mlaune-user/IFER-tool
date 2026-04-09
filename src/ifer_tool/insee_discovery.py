from __future__ import annotations

import gzip
import re
import unicodedata
from pathlib import Path
from urllib.parse import quote_plus, unquote, urljoin
from urllib.request import Request, urlopen

from ifer_tool.insee_types import (
    COG_INFO_PAGE_URL,
    DUCKDUCKGO_HTML_URL,
    HTTP_USER_AGENT,
    INSEE_BASE_URL,
    INSEE_SEARCH_URL,
    InseeArtifact,
    InseeError,
    UU_INFO_PAGE_URL,
)


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
    try:
        request = Request(url, headers={"User-Agent": HTTP_USER_AGENT}, method="HEAD")
        with urlopen(request, timeout=10):
            return True
    except Exception:
        pass

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
    for encoded_url in re.findall(r"uddg=([^&\"']+)", html_text):
        try:
            decoded = unquote(encoded_url)
        except Exception:
            continue
        if "insee.fr" not in decoded.lower():
            continue
        candidates.append(decoded)

    filtered = [u for u in candidates if "/fr/statistiques/" in u.lower() or "/fr/information/" in u.lower()]
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
