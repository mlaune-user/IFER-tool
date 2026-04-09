#!/bin/bash
set -e

cd /workspaces/IFER-tool

echo "🔍 État du repo avant nettoyage..."
git status

echo ""
echo "🧹 Suppression de insee_module.py de l'historique git..."

# Reset le fichier local d'abord
git reset HEAD src/ifer_tool/insee_module.py 2>/dev/null || true
git checkout -- src/ifer_tool/insee_module.py 2>/dev/null || true

# Nettoyer l'historique avec filter-branch
git filter-branch --index-filter \
  'git rm --cached --ignore-unmatch src/ifer_tool/insee_module.py' \
  HEAD

echo ""
echo "✅ insee_module.py retiré de l'historique"
echo ""
echo "📝 Suppression des fichiers temporaires..."
rm -f src/ifer_tool/insee_module.py
rm -f src/ifer_tool/insee_module_new.py
rm -f src/ifer_tool/insee_module_temp.py
rm -f src/ifer_tool/insee_module_clean.py
rm -f insee_module_fixed.py
rm -f cleanup_insee.py

echo "✅ Fichiers suppressMis"

echo ""
echo "📝 Etat final du repo..."
git status
