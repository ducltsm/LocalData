"""fb_pipeline — Phase 1: nạp raw GA4 từ BigQuery export vào ClickHouse.

Airflow chỉ orchestrate; mọi logic nằm ở package này. Python không parse dữ liệu —
ClickHouse tự đọc Parquet qua s3()/file().
"""
