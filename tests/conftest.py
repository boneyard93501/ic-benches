# tests/conftest.py
import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(root / "src"), str(root)]
