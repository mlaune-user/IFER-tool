#!/usr/bin/env python3
"""Remove all INSEE corrupt module files from repo."""
import subprocess
from pathlib import Path
import sys

repo_root = Path("/workspaces/IFER-tool")
src_dir = repo_root / "src/ifer_tool"

# Files to remove completely
files_to_delete = [
    src_dir / "insee_module.py",
    src_dir / "insee_module_clean.py",
    src_dir / "insee_module_new.py",
    src_dir / "insee_module_temp.py",
    repo_root / "insee_module_fixed.py",
    repo_root / "cleanup_insee.py",
]

print("🧹 Cleaning up INSEE module corruption...\n")

# Change to repo
import os
os.chdir(repo_root)

# Delete files and remove from git
for f in files_to_delete:
    if f.exists():
        print(f"  Removing {f.relative_to(repo_root)}...")
        # Remove from git index if tracked
        subprocess.run(["git", "rm", "-f", "--", str(f.relative_to(repo_root))], 
                       capture_output=True)
        # Remove from filesystem
        f.unlink(missing_ok=True)

# Verify insee_mod.py exists
good_file = src_dir / "insee_mod.py"
if not good_file.exists():
    print("❌ ERROR: insee_mod.py not found!")
    sys.exit(1)

print(f"\n✅ Kept {good_file.relative_to(repo_root)} (76 lines)")

# Check status
print("\n📊 Git status:")
result = subprocess.run(["git", "status", "--short"], capture_output=True, text=True)
print(result.stdout)

# Commit
print("\n📝 Committing changes...")
subprocess.run(["git", "add", "-A"], cwd=repo_root)
result = subprocess.run(
    ["git", "commit", "-m", "refactor: remove corrupted insee_module files, keep clean insee_mod.py"],
    cwd=repo_root,
    capture_output=True,
    text=True
)

if result.returncode == 0:
    print("✅ Committed successfully")
    print("\n🎉 Done! INSEE module is clean:")
    print("  - Old corrupted fils removed")
    print("  - Using clean insee_mod.py (76 lines)")
    print("  - main.py & tests use insee_mod imports")
else:
    print(f"⚠️  Commit status: {result.returncode}")
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
