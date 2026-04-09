from __future__ import annotations

import shutil
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import duckdb

DuckDBConnection = duckdb.DuckDBPyConnection


class DuckDBAdapterError(RuntimeError):
    pass


@dataclass(frozen=True)
class DuckDBLoadResult:
    database_path: Path
    loaded_tables: dict[str, int]
    extracted_files: list[Path]


def open_duckdb_connection(database_path: Path) -> DuckDBConnection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(database_path))


def get_table_columns(connection: DuckDBConnection, table_name: str) -> list[str]:
    rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return [row[1] for row in rows]


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _quote_string(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _sanitize_identifier(value: str) -> str:
    sanitized = "".join(character.lower() if character.isalnum() else "_" for character in value)
    sanitized = "_".join(part for part in sanitized.split("_") if part)
    if not sanitized:
        return "table_data"
    if sanitized[0].isdigit():
        return f"t_{sanitized}"
    return sanitized


def _find_archives(source_dir: Path) -> list[Path]:
    archives = sorted(source_dir.glob("*.zip"))
    if not archives:
        raise DuckDBAdapterError(f"Aucune archive ZIP trouvée dans '{source_dir}'.")
    return archives


def _extract_archives(archive_paths: list[Path], extract_dir: Path) -> list[Path]:
    extracted_csv_files: list[Path] = []

    extract_dir.mkdir(parents=True, exist_ok=True)

    for archive_path in archive_paths:
        destination_dir = extract_dir / archive_path.stem
        if destination_dir.exists():
            shutil.rmtree(destination_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(destination_dir)

        extracted_csv_files.extend(
            sorted(path for path in destination_dir.rglob("*") if path.is_file() and path.suffix.lower() == ".csv")
        )

    if not extracted_csv_files:
        raise DuckDBAdapterError("Aucun fichier CSV trouvé dans les archives ANFR téléchargées.")

    return extracted_csv_files


def _group_csv_files_by_table(csv_files: list[Path], extract_dir: Path) -> dict[str, list[Path]]:
    grouped_files: dict[str, list[Path]] = defaultdict(list)

    for csv_file in csv_files:
        relative_path = csv_file.relative_to(extract_dir)
        logical_path = relative_path.with_suffix("")
        table_name = _sanitize_identifier("_".join(logical_path.parts[1:]) or logical_path.stem)
        grouped_files[table_name].append(csv_file)

    return dict(grouped_files)


def _load_grouped_csv_files(
    grouped_files: dict[str, list[Path]],
    database_path: Path,
    schema: str,
) -> dict[str, int]:
    loaded_tables: dict[str, int] = {}

    with open_duckdb_connection(database_path) as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")
        connection.execute(f"SET schema = {_quote_string(schema)}")

        for table_name, csv_files in sorted(grouped_files.items()):
            placeholders = ", ".join("?" for _ in csv_files)
            sql = f"""
                CREATE OR REPLACE TABLE {_quote_identifier(table_name)} AS
                SELECT *
                FROM read_csv_auto(
                    [{placeholders}],
                    all_varchar = true,
                    union_by_name = true,
                    sample_size = -1,
                    normalize_names = true,
                    filename = true
                )
            """
            connection.execute(sql, [str(csv_file) for csv_file in csv_files])
            loaded_tables[f"{schema}.{table_name}"] = len(csv_files)

    return loaded_tables


def load_anfr_archives_to_duckdb(
    source_dir: Path,
    database_path: Path,
    extract_dir: Path,
    schema: str = "anfr",
) -> DuckDBLoadResult:
    archive_paths = _find_archives(source_dir)
    csv_files = _extract_archives(archive_paths, extract_dir)
    grouped_files = _group_csv_files_by_table(csv_files, extract_dir)
    loaded_tables = _load_grouped_csv_files(grouped_files, database_path, schema)
    return DuckDBLoadResult(
        database_path=database_path,
        loaded_tables=loaded_tables,
        extracted_files=csv_files,
    )