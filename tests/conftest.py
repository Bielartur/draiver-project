# tests/conftest.py
import sys
from pathlib import Path

# Compute absolute path to the src/ directory
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR      = PROJECT_ROOT / "src"

# Insert it at the front of sys.path so Python can find your packages
sys.path.insert(0, str(SRC_DIR))
