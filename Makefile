# ============================================================================
# Firebase (GA4) -> ClickHouse raw pipeline — Phase 1
# Windows: chạy trong Git Bash/WSL với GNU make (winget install ezwinports.make)
# ============================================================================
COMPOSE := docker compose
AF_EXEC := $(COMPOSE) exec airflow-scheduler
CH_EXEC := $(COMPOSE) exec clickhouse

.PHONY: up down restart logs ps build init ch-cli ch-schema airflow-cli test lint \
        dag-test clean sample-parquet peek explore-keys flatten _need-date

up: ## Build + khởi động toàn bộ stack, apply schema ClickHouse
	$(COMPOSE) up -d --build --wait
	$(MAKE) ch-schema
	@echo "Airflow UI: http://localhost:8080 — ClickHouse HTTP: http://localhost:8123"

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart

logs:
	$(COMPOSE) logs -f --tail 200

ps:
	$(COMPOSE) ps

build:
	$(COMPOSE) build

init: ## Tạo .env từ .env.example (không ghi đè) + nhắc đặt key
	@test -f .env || cp .env.example .env
	@echo "==> Đã có .env. Việc cần làm tay:"
	@echo "    1. Điền CLICKHOUSE_PASSWORD, GCS_HMAC_ACCESS_KEY/SECRET vào .env"
	@echo "    2. Đặt key service account vào secrets/gcp-sa.json"

ch-cli: ## Mở clickhouse-client trong container
	$(CH_EXEC) bash -c 'clickhouse-client -u "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD"'

ch-schema: ## Apply lại clickhouse/sql/ (idempotent) + TTL theo RAW_TTL_DAYS
	$(AF_EXEC) python -m fb_pipeline.tools.apply_schema

airflow-cli: ## Ví dụ: make airflow-cli CMD="dags list"
	$(AF_EXEC) airflow $(CMD)

test: ## Chạy pytest (unit + integration) trong container Airflow
	$(AF_EXEC) bash -c 'cd /opt/fb_pipeline && python -m pytest tests -v'

lint:
	$(AF_EXEC) bash -c 'cd /opt/fb_pipeline && ruff check src tests && python -m compileall -q src /opt/airflow/dags'

dag-test: _need-date ## make dag-test DATE=2026-08-27 — chạy DAG daily cho 1 ngày, không cần scheduler
	$(AF_EXEC) airflow dags test firebase_raw_daily $(DATE)

# ----------------------------------------------------------------------------
# Ba target debug đặc thù
# ----------------------------------------------------------------------------
sample-parquet: _need-date ## Tải 1 file Parquet thật + DESCRIBE + diff với source_schema.py
	$(AF_EXEC) python -m fb_pipeline.tools.sample_parquet --date $(DATE)

peek: _need-date ## Xem 1 dòng đầy đủ của partition
	$(CH_EXEC) bash -c 'clickhouse-client -u "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD" --param_dt="$(DATE)" -q "SELECT * FROM fb.events_raw WHERE _dt = {dt:Date} LIMIT 1 FORMAT Vertical"'

explore-keys: _need-date ## Read-only: thống kê key/kiểu của event_params & user_properties
	$(AF_EXEC) python -m fb_pipeline.tools.explore_keys --date $(DATE)

flatten: _need-date ## Flatten lại 1 ngày từ events_raw vào events_flat (không đụng BigQuery)
	$(AF_EXEC) python -m fb_pipeline.tools.flatten_day --date $(DATE)

clean: ## Xoá container + VOLUME (mất dữ liệu ClickHouse!) — có xác nhận
	@printf "Xoá toàn bộ container + volume (MẤT dữ liệu ClickHouse)? [y/N] " && read ans && [ "$$ans" = "y" ]
	$(COMPOSE) down -v

_need-date:
	@test -n "$(DATE)" || { echo "Thiếu tham số: DATE=YYYY-MM-DD"; exit 1; }
