from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

DATA_GOUV_DATASETS_URL = "https://www.data.gouv.fr/api/1/datasets/"
DEFAULT_QUERY = "installations radioelectriques 5w"
FALLBACK_QUERIES = (
    DEFAULT_QUERY,
    "installations radioélectriques 5w",
    "installations radioelectriques plus de 5 watts",
    "installations radioélectriques plus de 5 watts",
    "donnees installations radioelectriques plus de 5 watts",
    "données sur les installations radioélectriques de plus de 5 watts",
    "anfr installations radioélectriques",
)


class DataGouvError(RuntimeError):
    pass


def _normalize(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_only).strip().lower()


def _stringify_fields(*values: Any) -> str:
    return " ".join(str(value) for value in values if value)


def fetch_json(url: str) -> dict[str, Any]:
    with urlopen(url) as response:
        return json.load(response)


def search_datasets(query: str = DEFAULT_QUERY) -> list[dict[str, Any]]:
    seen_ids: set[str] = set()
    datasets: list[dict[str, Any]] = []
    queries = [query, *[item for item in FALLBACK_QUERIES if item != query]]

    for current_query in queries:
        api_url = f"{DATA_GOUV_DATASETS_URL}?{urlencode({'q': current_query, 'page_size': 20})}"
        payload = fetch_json(api_url)
        current_datasets = payload.get("data") or payload.get("results") or []
        for dataset in current_datasets:
            dataset_id = str(dataset.get("id") or dataset.get("slug") or dataset.get("page") or "")
            if dataset_id in seen_ids:
                continue
            seen_ids.add(dataset_id)
            datasets.append(dataset)

    if not datasets:
        raise DataGouvError(f"Aucun dataset trouvé pour la requête '{query}'.")
    return datasets


def _dataset_score(dataset: dict[str, Any]) -> tuple[int, int]:
    searchable_text = _normalize(
        _stringify_fields(
            dataset.get("title"),
            dataset.get("description"),
            dataset.get("slug"),
            dataset.get("page"),
        )
    )
    score = 0
    for term, weight in (
        ("installation", 3),
        ("radioelectrique", 4),
        ("5w", 5),
        ("plus de 5w", 6),
        ("refer", 1),
    ):
        if term in searchable_text:
            score += weight
    return score, len(dataset.get("resources") or [])


def find_target_dataset(query: str = DEFAULT_QUERY) -> dict[str, Any]:
    datasets = search_datasets(query=query)
    ranked_datasets = sorted(datasets, key=_dataset_score, reverse=True)
    best_match = ranked_datasets[0]
    if _dataset_score(best_match)[0] == 0:
        raise DataGouvError("Impossible d'identifier le dataset cible sur data.gouv.fr.")
    return best_match


def find_reference_resources(
    dataset: dict[str, Any],
    year: int = 2026,
) -> list[dict[str, Any]]:
    resources = dataset.get("resources") or []
    matching_resources: list[dict[str, Any]] = []
    normalized_year = str(year)

    for resource in resources:
        searchable_text = _normalize(
            _stringify_fields(
                resource.get("title"),
                resource.get("description"),
                resource.get("type"),
                resource.get("format"),
                resource.get("mime"),
                resource.get("url"),
                resource.get("latest"),
            )
        )

        if normalized_year not in searchable_text:
            continue
        if "table" not in searchable_text:
            continue
        if "refer" not in searchable_text:
            continue

        matching_resources.append(resource)

    if not matching_resources:
        dataset_title = dataset.get("title") or "<sans titre>"
        raise DataGouvError(
            f"Aucune table de références {year} trouvée dans le dataset '{dataset_title}'."
        )

    return sorted(matching_resources, key=lambda resource: resource.get("title") or "")


def _safe_filename(resource: dict[str, Any]) -> str:
    source_name = resource.get("title") or resource.get("id") or "resource"
    normalized_name = _normalize(source_name)
    slug = re.sub(r"[^a-z0-9]+", "-", normalized_name).strip("-") or "resource"

    resource_url = resource.get("url") or resource.get("latest") or ""
    suffix = Path(urlparse(resource_url).path).suffix
    if not suffix:
        format_name = (resource.get("format") or resource.get("type") or "bin").lower()
        suffix = f".{format_name}"

    return f"{slug}{suffix}"


def download_resource(resource: dict[str, Any], output_dir: Path) -> Path:
    resource_url = resource.get("url") or resource.get("latest")
    if not resource_url:
        resource_title = resource.get("title") or resource.get("id") or "<sans titre>"
        raise DataGouvError(f"La ressource '{resource_title}' ne contient pas d'URL.")

    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / _safe_filename(resource)
    with urlopen(resource_url) as response:
        destination.write_bytes(response.read())
    return destination


def fetch_reference_tables(
    output_dir: Path,
    year: int = 2026,
    query: str = DEFAULT_QUERY,
) -> list[Path]:
    dataset = find_target_dataset(query=query)
    resources = find_reference_resources(dataset=dataset, year=year)
    return [download_resource(resource, output_dir) for resource in resources]