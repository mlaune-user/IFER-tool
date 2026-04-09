from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


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


def ensure_insee_dir(path: Path) -> Path:
    return path if path.name == "insee" else path / "insee"
