"""Remove installation caches and optional browser examples from the worker base."""
import importlib.metadata
import os
from pathlib import Path
import shutil
import subprocess
import sys

prefix = Path("/opt/conda")
assert Path(sys.prefix).resolve() == prefix, "This script is only for the container build"
packages = sorted({
    distribution.metadata["Name"]
    for distribution in importlib.metadata.distributions()
    if distribution.metadata["Name"].lower().replace("_", "-").startswith("comfyui-workflow-templates")
    or distribution.metadata["Name"].lower().replace("_", "-") == "comfyui-embedded-docs"
})
if packages:
    subprocess.run([sys.executable, "-m", "pip", "uninstall", "--yes", *packages], check=True)
shutil.rmtree(prefix / "pkgs", ignore_errors=True)
# Preserve the already-published GPU library layers. Bytecode in the remaining
# base tree is disposable and otherwise creates thousands of extraction writes.
site = prefix / "lib/python3.11/site-packages"
preserved = {site / "torch", site / "nvidia"}
removed = 0
for directory, children, _ in os.walk(prefix):
    parent = Path(directory)
    for name in list(children):
        child = parent / name
        if child in preserved:
            children.remove(name)
        elif name == "__pycache__":
            shutil.rmtree(child)
            children.remove(name)
            removed += 1
print(f"Removed {len(packages)} optional browser packages and {removed} bytecode directories")
