from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Dict, Any, List

import pendulum
import requests
from airflow.decorators import dag, task

API_URL = "https://api-open.data.gov.sg/v2/real-time/api/wind-speed"
kan = 'kan'

@dag(
    dag_id="save_wind_speed_to_csv",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Bangkok"),
    schedule=None,
    catchup=False,
    tags=["example", "data.gov.sg", "wind-speed", "csv"],
    default_args={"retries": 2},
    dag_display_name="Save SG wind-speed stations to CSV",
)
def save_wind_speed_to_csv():
    """Fetch station metadata for today's date and save to a CSV in ../save.

    Output: ../save/wind_speed_<today>.csv
    Always writes a CSV (even if empty) and a debug JSON alongside for troubleshooting.
    """

    @task(task_id="fetch_and_save")
    def fetch_and_save() -> str:
        from airflow.utils.log.logging_mixin import LoggingMixin
        import json
        from datetime import date

        logger = LoggingMixin().log

        save_dir = Path(__file__).resolve().parent / "station"
        save_dir.mkdir(parents=True, exist_ok=True)

        today_str = date.today().strftime("%Y-%m-%d")

        out_csv = save_dir / f"station.csv"

        params = {"date": today_str}
        logger.info("Requesting %s with params=%s", API_URL, params)

        payload: Dict[str, Any] = {}
        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            logger.info("HTTP status=%s", resp.status_code)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.exception("Failed to GET API: %s", e)
            fieldnames = ["id", "device_id", "name", "latitude", "longitude"]
            with out_csv.open("w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=fieldnames).writeheader()
            raise

        code = payload.get("code")
        data = payload.get("data") or {}
        stations: List[Dict[str, Any]] = data.get("stations") or []

        if code != 0:
            logger.warning("API returned non-zero code=%s; will still write CSV header only.", code)

        rows = []
        for st in stations:
            loc = (st.get("location") or {})
            rows.append({
                "id": st.get("id"),
                "device_id": st.get("deviceId"),
                "name": st.get("name"),
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
            })

        fieldnames = ["id", "device_id", "name", "latitude", "longitude"]
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            if rows:
                writer.writerows(rows)
                logger.info("Wrote %d rows to %s", len(rows), out_csv)
            else:
                logger.warning("No station rows; wrote header only to %s", out_csv)

        return str(out_csv)

    fetch_and_save()


dag_object = save_wind_speed_to_csv()
