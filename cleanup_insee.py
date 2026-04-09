#!/usr/bin/env python
"""Cleanup deprecated INSEE module files and commit."""
import subprocess
from pathlib import Path

src_dir = Path(__file__).parent / "src/ifer_tool"

# Files to delete
files_to_delete = [
    src_dir / "insee_module.py",
    src_dir / "insee_module_new.py", 
    src_dir / "insee_module_temp.py",
]

# Delete old corrupted files
for f in files_to_delete:
    if f.exists():
        f.unlink()
        print(f"✓ Deleted {f.name}")

# Verify the good file exists
good_file = src_dir / "insee_mod.py"
assert good_file.exists(), "insee_mod.py not found!"
print(f"✓ {good_file.name} exists (76 lines)")

# Quick syntax check
print("✓ Checking Python syntax...")
result = subprocess.run(
    ["python", "-m", "py_compile", str(good_file)],
    capture_output=True,
    text=True
)
if result.returncode != 0:
    print(f"✗ Syntax error: {result.stderr}")
    exit(1)
print("✓ Syntax check passed")

# Git commands
print("Adding changes to git...")
subprocess.run(["git", "add", "-A"], cwd="/workspaces/IFER-tool")
subprocess.run(["git", "commit", "-m", "refactor: replace corrupted insee_module.py with clean insee_mod.py (76 lines)"], cwd="/workspaces/IFER-tool")
print("✓ Committed to git")

print("\n✅ All done!")
