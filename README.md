# IFER-tool

## Run
```bash
PYTHONPATH=src python -m ifer_tool.main
```

## Télécharger les tables de références 2026
```bash
PYTHONPATH=src python -m ifer_tool.main
```

La routine interroge l'API data.gouv.fr pour trouver le dataset des installations radioélectriques de plus de 5W, filtre les ressources contenant une table de références pour l'année demandée, puis télécharge les fichiers correspondants dans le répertoire anfr à la racine du projet.

## Charger les archives ANFR dans DuckDB
```bash
PYTHONPATH=src python -m ifer_tool.main load-duckdb
```

Toute la dépendance DuckDB est isolée dans [src/ifer_tool/duckdb_adapter.py](src/ifer_tool/duckdb_adapter.py). Le reste du projet appelle seulement une fonction de chargement, ce qui limite l'impact d'un futur remplacement par BigQuery ou un autre moteur.

## Construire la table INSEE COG -> TUU/TDUU
```bash
PYTHONPATH=src python -m ifer_tool.main insee-build --insee-year 2025 --expected-rows 39071
```

Le module INSEE est isolé dans [src/ifer_tool/insee_module.py](src/ifer_tool/insee_module.py). Il recherche les derniers fichiers INSEE disponibles, récupère le COG de l'année cible (ou n-1 en fallback), récupère la base unités urbaines 2020 et l'historique des communes depuis 1943, puis construit une table DuckDB de correspondance.
