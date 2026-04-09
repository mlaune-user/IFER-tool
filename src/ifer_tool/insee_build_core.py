from __future__ import annotations

import re
import shutil
import zipfile
from pathlib import Path

from ifer_tool.duckdb_adapter import DuckDBConnection, get_table_columns
from ifer_tool.insee_types import COG_CODE_REGEX, InseeError, TABULAR_FILE_SUFFIXES


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

    raise InseeError(f"Aucune colonne compatible trouvée. Colonnes disponibles: {', '.join(columns)}")


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
    if suffix in {".xls", ".xlsx", ".dbf", ".shp", ".gpkg", ".geojson"}:
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
        path for path in directory.rglob("*") if path.is_file() and path.suffix.lower() in TABULAR_FILE_SUFFIXES
    )
    if not candidates:
        return None
    csv_candidates = [path for path in candidates if path.suffix.lower() == ".csv"]
    return csv_candidates[0] if csv_candidates else candidates[0]


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

        nested_archives = sorted(path for path in extract_root.rglob("*") if path.is_file() and path.suffix.lower() == ".zip")
        for nested_archive in nested_archives:
            try:
                return _prepare_tabular_file(nested_archive, _depth=_depth + 1)
            except InseeError:
                continue

        raise InseeError(f"Aucun fichier tabulaire exploitable trouvé dans l'archive '{file_path.name}'.")

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
    connection.execute(f"CREATE OR REPLACE TEMP TABLE history_raw AS SELECT * FROM {_open_relation_sql(history_path)}")

    cog_columns = get_table_columns(connection, "cog_raw")
    uu_columns = get_table_columns(connection, "uu_raw")
    history_columns = get_table_columns(connection, "history_raw")

    cog_code_column = _pick_column(cog_columns, ["COM", "CODGEO", "CODE_COMMUNE", "DEP_COM"])
    cog_type_column = _pick_column(cog_columns, ["TYPECOM", "TYPE_COM"])
    cog_parent_column = _pick_column(cog_columns, ["COMPARENT", "COM_PARENT", "PARENT", "COM_AP"])
    uu_code_column = _pick_column(uu_columns, ["COM", "CODGEO", "CODE_COMMUNE", "DEP_COM"])
    tuu_column = _pick_column(uu_columns, ["TUU2020", "TUU", "UU2020", "TYPE_UNITE_URBAINE"])
    tduu_column = _pick_column(uu_columns, ["TDUU2020", "TDUU", "TYPE_DETAILLE_UNITE_URBAINE", "TYPE_COM", "LIBUU2020"])

    normalized_history_columns = {_normalize_column_name(column) for column in history_columns}
    history_is_snapshot = all(column in normalized_history_columns for column in ("com", "datedebut", "datefin"))

    hist_old_column = None
    hist_new_column = None
    if not history_is_snapshot:
        hist_old_column = _pick_column(history_columns, ["COM_AV", "DEP_COM_AV", "CODGEO_AV", "OLD_COM", "CODE_AVANT", "OLD_CODE"])
        hist_new_column = _pick_column(history_columns, ["COM_AP", "DEP_COM_AP", "CODGEO_AP", "NEW_COM", "CODE_APRES", "CODGEO", "NEW_CODE"])

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

    connection.execute("CREATE OR REPLACE TEMP TABLE cog_current AS SELECT DISTINCT code FROM cog_resolution")
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE arrondissement_resolution AS
        SELECT DISTINCT code AS cog_initial, code AS cog_reference
        FROM cog_resolution
        WHERE upper(coalesce(type_code, '')) = 'ARM'
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE uu_base AS
        SELECT DISTINCT {uu_code_column} AS code, {tuu_column} AS tuu, {tduu_column} AS tduu
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
            SELECT DISTINCT {hist_old_column} AS old_code, {hist_new_column} AS new_code
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
            SELECT history.code AS cog_initial, coalesce(resolution.reference_code, history.code) AS cog_reference
            FROM history_codes history
            LEFT JOIN cog_resolution resolution ON resolution.code = history.code
            UNION
            SELECT resolution.code AS cog_initial, resolution.reference_code AS cog_reference
            FROM cog_resolution resolution
            UNION
            SELECT cog_initial, cog_reference
            FROM arrondissement_resolution
        ), mapping AS (
            SELECT DISTINCT cog_initial, cog_reference FROM all_codes
        )
        SELECT mapping.cog_initial, mapping.cog_reference, uu.tuu, uu.tduu,
               {target_year} AS cog_year, {target_year} AS target_year
        FROM mapping
        LEFT JOIN uu_base uu ON uu.code = mapping.cog_reference
        {metro_filter}
        """
    )

    row_count = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    return f"insee.{table_name}", int(row_count)
