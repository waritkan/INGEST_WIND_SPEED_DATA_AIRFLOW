from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Any, List

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

API_URL = "https://api-open.data.gov.sg/v2/real-time/api/wind-speed"
OUTPUT_DIR = Path(__file__).resolve().parent / "wind_speed_data"

# สร้างตาราง + ดัชนี และ Unique constraint กันข้อมูลซ้ำ
TABLE_SQL = """
IF OBJECT_ID('dbo.wind_speed_data', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.wind_speed_data(
        id INT IDENTITY(1,1) PRIMARY KEY,
        time_stamp DATE NOT NULL,
        station_id VARCHAR(32) NOT NULL,
        wind_speed DECIMAL(6,2) NOT NULL
    );
    CREATE INDEX IX_wind_speed_data_ts ON dbo.wind_speed_data(time_stamp);
END;
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UQ_wind_speed_data_ts_station'
      AND object_id = OBJECT_ID('dbo.wind_speed_data')
)
BEGIN
    CREATE UNIQUE INDEX UQ_wind_speed_data_ts_station
    ON dbo.wind_speed_data(time_stamp, station_id);
END;
"""

@dag(
    dag_id="ingest_wind_speed_daily",
    start_date=pendulum.datetime(2025, 1, 1, 0, 5, tz="Asia/Bangkok"),
    schedule="0 12 * * *",   # เที่ยงตาม timezone ของ Airflow (ตั้ง DEFAULT_TIMEZONE เป็น Asia/Bangkok จะตรง)
    catchup=False,
    tags=["example", "data.gov.sg", "wind-speed", "daily"],
    dag_display_name="Ingest SG wind-speed data daily",
    max_active_runs=1,
)
def ingest_wind_speed_daily():

    @task(task_id="ingest")
    def ingest():
        # ใช้เวลา Singapore ตามสเปก API
        now_sg = pendulum.now("Asia/Singapore")
        date_str = now_sg.format("YYYY-MM-DD")
        # API นี้ใช้พารามิเตอร์ระดับชั่วโมง: นาที fix เป็น 01
        date_str_param = now_sg.format("YYYY-MM-DD[T]HH:01:00")

        resp = requests.get(API_URL, params={"date": date_str_param}, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        if payload.get("code") != 0:
            raise ValueError(f"Unexpected API response code: {payload.get('code')}")

        readings = (payload.get("data") or {}).get("readings") or []
        if not isinstance(readings, list):
            raise TypeError(f"`data.readings` should be a list, got: {type(readings)}")

        rows: List[Dict[str, Any]] = []
        for bucket in readings:
            ts = (bucket.get("timestamp") or "").split("T")[0]  # YYYY-MM-DD
            per_station = bucket.get("data") or []
            if not isinstance(per_station, list):
                continue
            for r in per_station:
                val = r.get("value")
                # บางจุดไม่มีค่า ข้าม
                if val in (None, ""):
                    continue
                rows.append({
                    "time_stamp": ts,
                    "station_id": (r.get("stationId") or r.get("station_id") or "").strip(),
                    "wind_speed": float(val),
                })

        return {"time_stamp": date_str, "rows": rows}

    @task(task_id="save_incremental_csv")
    def save_incremental_csv(batch):
        save_dir = OUTPUT_DIR
        save_dir.mkdir(parents=True, exist_ok=True)

        time_stamp = batch["time_stamp"]
        rows: List[Dict[str, Any]] = batch.get("rows", [])

        out_csv = save_dir / f"wind_speed_{time_stamp}.csv"
        fieldnames = ["time_stamp", "station_id", "wind_speed"]
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            if rows:
                w.writerows(rows)
        return str(out_csv)

    @task(task_id="save_full_sql_database")
    def save_full_sql_database(batch):
        rows: List[Dict[str, Any]] = batch.get("rows", [])

        # สร้างตาราง/ดัชนี/Unique index ถ้ายังไม่มี
        hook = MsSqlHook(mssql_conn_id="mssql_default")
        hook.run(TABLE_SQL)

        if not rows:
            return "upserted_rows=0"

        # ทำความสะอาดข้อมูล
        cleaned: List[tuple] = []
        for r in rows:
            ts = r.get("time_stamp")
            sid = (r.get("station_id") or "").strip()
            v = r.get("wind_speed")
            if not (ts and sid and v is not None):
                continue
            cleaned.append((ts, sid, round(float(v), 2)))

        if not cleaned:
            return "upserted_rows=0"

        # ใช้ temp table + MERGE ทำ upsert (กันซ้ำแบบปลอดภัย)
        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                IF OBJECT_ID('tempdb..#staging_wind', 'U') IS NOT NULL DROP TABLE #staging_wind;
                CREATE TABLE #staging_wind (
                    time_stamp DATE NOT NULL,
                    station_id VARCHAR(32) NOT NULL,
                    wind_speed DECIMAL(6,2) NOT NULL
                );
            """)
            cur.executemany(
                "INSERT INTO #staging_wind (time_stamp, station_id, wind_speed) VALUES (%s,%s,%s)",
                cleaned
            )
            cur.execute("""
                MERGE dbo.wind_speed_data AS tgt
                USING #staging_wind AS src
                   ON tgt.time_stamp = src.time_stamp
                  AND tgt.station_id = src.station_id
                WHEN MATCHED THEN
                    UPDATE SET wind_speed = src.wind_speed
                WHEN NOT MATCHED THEN
                    INSERT (time_stamp, station_id, wind_speed)
                    VALUES (src.time_stamp, src.station_id, src.wind_speed);
            """)
        conn.commit()
        return f"upserted_rows={len(cleaned)}"

    data = ingest()
    save_incremental_csv(data) >> save_full_sql_database(data)

dag_object = ingest_wind_speed_daily()