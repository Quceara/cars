from __future__ import annotations

import math
import os
import threading
import json
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.database import replace_cars_and_meta
from app.parser import DEFAULT_BATCH_SIZE, DEFAULT_QUERY, collect_all_batches, collect_all_cars_segmented

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "encar_cars.json"
DEFAULT_META_PATH = PROJECT_ROOT / "data" / "encar_meta.json"

DAILY_HOUR = 3
DAILY_MINUTE = 0

_scheduler = BackgroundScheduler()
_job_lock = threading.Lock()
TEST_LIMIT_ENV = "ENCAR_TEST_LIMIT"


def _get_test_limit() -> int | None:
    raw_value = os.getenv(TEST_LIMIT_ENV)
    if not raw_value:
        return None
    try:
        value = int(raw_value)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


def update_encar_data() -> None:
    if not _job_lock.acquire(blocking=False):
        print("ENCAR update skipped: previous run is still active.")
        return

    try:
        print("ENCAR update started.")
        cars: list[dict[str, object]]
        test_limit = _get_test_limit()
        if test_limit is not None:
            max_batches = max(1, math.ceil(test_limit / DEFAULT_BATCH_SIZE))
            print(f"ENCAR test mode: parsing limited to about {test_limit} records.")
            cars = collect_all_batches(
                batch_size=DEFAULT_BATCH_SIZE,
                max_batches=max_batches,
                query=DEFAULT_QUERY,
                output_path=str(DEFAULT_OUTPUT_PATH),
                metadata_path=str(DEFAULT_META_PATH),
            )
        else:
            cars = collect_all_cars_segmented(
                output_path=str(DEFAULT_OUTPUT_PATH),
                metadata_path=str(DEFAULT_META_PATH),
                batch_size=DEFAULT_BATCH_SIZE,
            )
        meta: dict[str, object] = {}
        if DEFAULT_META_PATH.exists():
            try:
                with open(DEFAULT_META_PATH, "r", encoding="utf-8") as file:
                    loaded = json.load(file)
                    if isinstance(loaded, dict):
                        meta = loaded
            except (json.JSONDecodeError, OSError):
                meta = {}
        replace_cars_and_meta(cars, meta)
        print("ENCAR update finished.")
    except Exception as exc:
        print(f"ENCAR update failed: {exc}")
    finally:
        _job_lock.release()


def start_scheduler() -> None:
    if _scheduler.running:
        return

    _scheduler.add_job(
        update_encar_data,
        trigger=CronTrigger(hour=DAILY_HOUR, minute=DAILY_MINUTE),
        id="encar_daily_update",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    _scheduler.start()
    print(
        f"Scheduler started. Daily ENCAR update at "
        f"{DAILY_HOUR:02d}:{DAILY_MINUTE:02d}."
    )


def stop_scheduler() -> None:
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
        print("Scheduler stopped.")
