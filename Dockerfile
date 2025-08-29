FROM apache/airflow:2.9.0

USER airflow

COPY requirements.txt .

RUN PY_VER=$(python -c 'import sys; print(".".join(map(str, sys.version_info[:2])))') \
 && pip install --no-cache-dir \
    -r requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-${PY_VER}.txt"
