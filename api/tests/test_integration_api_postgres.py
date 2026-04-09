import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.integration

if os.getenv("RUN_INTEGRATION") != "1":
    pytest.skip(
        "Set RUN_INTEGRATION=1 to run integration tests against Docker services.",
        allow_module_level=True,
    )


REPO_ROOT = Path(__file__).resolve().parents[2]
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8001")
TEST_DB_NAME = os.getenv("TEST_DB_NAME", "featurestore_test")


def _run_compose_exec(*args: str) -> str:
    command = ["docker", "compose", "exec", "-T", *args]
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(command)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


def _run_compose(*args: str) -> str:
    command = ["docker", "compose", *args]
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(command)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


def _wait_for_api() -> None:
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            response = httpx.get(f"{API_BASE_URL}/health", timeout=3.0)
            if response.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError(f"API did not become healthy at {API_BASE_URL} within timeout")


@pytest.fixture(scope="module", autouse=True)
def ensure_integration_stack() -> None:
    _run_compose("--profile", "test", "up", "-d", "postgres", "api-test")
    _wait_for_api()


@pytest.fixture(scope="module", autouse=True)
def seed_featurestore_data() -> None:
    db_exists = _run_compose_exec(
        "postgres",
        "psql",
        "-U",
        "airflow",
        "-d",
        "postgres",
        "-tAc",
        f"SELECT 1 FROM pg_database WHERE datname='{TEST_DB_NAME}'",
    ).strip()

    if db_exists != "1":
        _run_compose_exec(
            "postgres",
            "psql",
            "-U",
            "airflow",
            "-d",
            "postgres",
            "-c",
            f"CREATE DATABASE {TEST_DB_NAME}",
        )

    sql = """
    CREATE TABLE IF NOT EXISTS wells (
        idpozo VARCHAR NOT NULL,
        sigla VARCHAR,
        empresa VARCHAR,
        provincia VARCHAR,
        cuenca VARCHAR,
        areayacimiento VARCHAR,
        fecha_data DATE
    );

    CREATE TABLE IF NOT EXISTS production (
        id SERIAL PRIMARY KEY,
        idpozo VARCHAR NOT NULL,
        fecha DATE NOT NULL,
        prod_pet FLOAT,
        prod_gas FLOAT,
        prod_agua FLOAT,
        tef FLOAT,
        tipoextraccion VARCHAR
    );

    TRUNCATE TABLE production RESTART IDENTITY;
    TRUNCATE TABLE wells;

    INSERT INTO wells (idpozo, sigla, fecha_data) VALUES
        ('POZO-INT-001', 'PINT1', '2023-10-01'),
        ('POZO-INT-002', 'PINT2', '2023-11-01');

    INSERT INTO production (idpozo, fecha, prod_gas, prod_pet, prod_agua, tef, tipoextraccion) VALUES
        ('POZO-INT-001', '2023-10-01', 100.5, 0, 0, 0, 'N/A'),
        ('POZO-INT-001', '2023-11-01', 110.0, 0, 0, 0, 'N/A'),
        ('POZO-INT-001', '2023-12-01', 115.25, 0, 0, 0, 'N/A');
    """

    _run_compose_exec(
        "postgres",
        "psql",
        "-U",
        "airflow",
        "-d",
        TEST_DB_NAME,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    )


def test_api_healthcheck_live() -> None:
    response = httpx.get(f"{API_BASE_URL}/health", timeout=10.0)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_wells_endpoint_with_live_postgres_data() -> None:
    response = httpx.get(
        f"{API_BASE_URL}/api/v1/wells",
        params={"date_query": "2023-10-15"},
        timeout=10.0,
    )

    assert response.status_code == 200
    assert response.json() == [{"id_well": "POZO-INT-001"}]


def test_forecast_endpoint_with_live_postgres_data() -> None:
    response = httpx.get(
        f"{API_BASE_URL}/api/v1/forecast",
        params={
            "id_well": "POZO-INT-001",
            "date_start": "2023-10-01",
            "date_end": "2023-11-30",
        },
        timeout=10.0,
    )

    assert response.status_code == 200
    assert response.json() == {
        "id_well": "POZO-INT-001",
        "data": [
            {"date": "2023-10-01", "prod": 100.5},
            {"date": "2023-11-01", "prod": 110.0},
        ],
    }
