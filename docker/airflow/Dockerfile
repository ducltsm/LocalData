# Image Airflow cho pipeline. KHÔNG COPY credential vào image —
# key JSON chỉ được mount lúc runtime (./secrets -> /opt/secrets, read-only).
FROM apache/airflow:2.10.5-python3.12

ENV FB_PIPELINE_HOME=/opt/fb_pipeline

# Providers + client libs, pin theo constraint chính thức của Airflow 2.10.5 / py3.12
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

# Cài fb_pipeline dạng editable; compose mount ./src đè lên để sửa code không cần rebuild.
# --no-deps vì runtime deps đã cài ở bước constraint phía trên.
COPY --chown=airflow:root pyproject.toml ${FB_PIPELINE_HOME}/pyproject.toml
COPY --chown=airflow:root src ${FB_PIPELINE_HOME}/src
COPY --chown=airflow:root clickhouse/sql ${FB_PIPELINE_HOME}/clickhouse/sql
COPY --chown=airflow:root tests ${FB_PIPELINE_HOME}/tests
RUN pip install --no-cache-dir --no-deps -e "${FB_PIPELINE_HOME}" \
 && pip install --no-cache-dir "pytest>=8,<9" "ruff>=0.6,<1" "pyarrow>=16"

WORKDIR ${FB_PIPELINE_HOME}
