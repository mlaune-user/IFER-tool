from __future__ import annotations

import argparse
from pathlib import Path

from ifer_tool.data_gouv import fetch_reference_tables
from ifer_tool.duckdb_adapter import load_anfr_archives_to_duckdb
from ifer_tool.insee_mod import build_insee_duckdb_table


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "anfr"
DEFAULT_DUCKDB_PATH = DEFAULT_OUTPUT_DIR / "anfr.duckdb"
DEFAULT_EXTRACT_DIR = DEFAULT_OUTPUT_DIR / "extracted"
DEFAULT_INSEE_DIR = PROJECT_ROOT / "insee"
DEFAULT_INSEE_DUCKDB_PATH = DEFAULT_INSEE_DIR / "insee.duckdb"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Télécharge les tables de références ANFR et peut les charger dans DuckDB."
        )
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=("fetch", "load-duckdb", "insee-build"),
        default="fetch",
        help="Action à exécuter : fetch, load-duckdb, insee-build.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Répertoire de destination des fichiers téléchargés.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2026,
        help="Année recherchée dans les ressources du dataset.",
    )
    parser.add_argument(
        "--query",
        default="installations radioelectriques 5w",
        help="Requête data.gouv.fr utilisée pour trouver le dataset cible.",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Répertoire source contenant les archives ZIP ANFR.",
    )
    parser.add_argument(
        "--database-path",
        type=Path,
        default=DEFAULT_DUCKDB_PATH,
        help="Fichier DuckDB de destination.",
    )
    parser.add_argument(
        "--extract-dir",
        type=Path,
        default=DEFAULT_EXTRACT_DIR,
        help="Répertoire de travail pour l'extraction des archives avant chargement.",
    )
    parser.add_argument(
        "--schema",
        default="anfr",
        help="Schéma DuckDB à utiliser pour les tables importées.",
    )
    parser.add_argument(
        "--insee-dir",
        type=Path,
        default=DEFAULT_INSEE_DIR,
        help="Répertoire de stockage des fichiers INSEE téléchargés.",
    )
    parser.add_argument(
        "--insee-database-path",
        type=Path,
        default=DEFAULT_INSEE_DUCKDB_PATH,
        help="Fichier DuckDB de destination pour la table INSEE.",
    )
    parser.add_argument(
        "--insee-year",
        type=int,
        default=2025,
        help="Année cible pour la table de correspondance COG -> TUU/TDUU.",
    )
    parser.add_argument(
        "--expected-rows",
        type=int,
        default=None,
        help="Contrôle de volumétrie attendu sur la table INSEE finale.",
    )
    parser.add_argument(
        "--expected-tolerance",
        type=int,
        default=0,
        help="Tolérance absolue autorisée sur le contrôle de volumétrie INSEE.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "load-duckdb":
        load_result = load_anfr_archives_to_duckdb(
            source_dir=args.source_dir,
            database_path=args.database_path,
            extract_dir=args.extract_dir,
            schema=args.schema,
        )
        print(load_result.database_path)
        for table_name, file_count in sorted(load_result.loaded_tables.items()):
            print(f"{table_name}: {file_count} fichier(s)")
        return

    if args.command == "insee-build":
        result = build_insee_duckdb_table(
            insee_dir=args.insee_dir,
            database_path=args.insee_database_path,
            target_year=args.insee_year,
            metro_only=True,
            expected_rows=args.expected_rows,
            expected_tolerance=args.expected_tolerance,
        )
        print(result.database_path)
        print(result.table_name)
        print(result.row_count)
        print(result.cog_artifact.local_path)
        print(result.uu_artifact.local_path)
        print(result.history_artifact.local_path)
        return

    downloaded_files = fetch_reference_tables(
        output_dir=args.output_dir,
        year=args.year,
        query=args.query,
    )
    for file_path in downloaded_files:
        print(file_path)


if __name__ == "__main__":
    main()
