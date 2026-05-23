"""Configura el sys.path para que los tests de Airflow encuentren los plugins."""

import sys
from pathlib import Path

# airflow/tests/ → airflow/plugins/
plugins_path = str(Path(__file__).parent.parent / "plugins")
if plugins_path not in sys.path:
    sys.path.insert(0, plugins_path)
