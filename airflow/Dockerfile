FROM apache/airflow:3.1.7

# Pinned task-level dependencies. Keeps container startup fast (no pip at runtime)
# and makes builds reproducible, unlike _PIP_ADDITIONAL_REQUIREMENTS.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
