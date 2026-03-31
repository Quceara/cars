import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.scheduler import start_scheduler, stop_scheduler, update_encar_data

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "encar_cars.json"
META_FILE = BASE_DIR / "data" / "encar_meta.json"

app = FastAPI(title="ENCAR Parser Service")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))
DEFAULT_PAGE_SIZE = 24
MAX_PAGE_SIZE = 100
TEST_LIMIT_ENV = "ENCAR_TEST_LIMIT"


def _build_car_dedup_key(car: dict[str, Any]) -> tuple[Any, ...]:
    return (
        car.get("марка"),
        car.get("модель"),
        car.get("год"),
        car.get("пробег"),
        car.get("цена"),
        car.get("фото"),
    )


def _normalize_photo_url(photo: Any) -> str | None:
    if not isinstance(photo, str) or not photo:
        return None
    normalized = photo
    if normalized.endswith("_"):
        normalized = f"{normalized}001.jpg"
    parsed = urlparse(normalized)
    if parsed.path and "." not in Path(parsed.path).name:
        normalized = f"{normalized}001.jpg"
    if normalized.startswith("http://") or normalized.startswith("https://"):
        return normalized
    if normalized.startswith("/"):
        return f"https://ci.encar.com{normalized}"
    return normalized


def _load_cars() -> list[dict[str, Any]]:
    if not DATA_FILE.exists():
        return []
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except (json.JSONDecodeError, OSError):
        return []

    if not isinstance(data, list):
        return []

    cars: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        car = {
            "марка": item.get("марка"),
            "модель": item.get("модель"),
            "год": item.get("год"),
            "пробег": item.get("пробег"),
            "цена": item.get("цена"),
            "фото": _normalize_photo_url(item.get("фото")),
        }
        key = _build_car_dedup_key(car)
        if key in seen:
            continue
        seen.add(key)
        cars.append(car)
    return cars


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


def _load_cars_with_optional_limit() -> list[dict[str, Any]]:
    cars = _load_cars()
    limit = _get_test_limit()
    if limit is None:
        return cars
    return cars[:limit]


def _load_meta() -> dict[str, Any]:
    if not META_FILE.exists():
        return {}
    try:
        with open(META_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except (json.JSONDecodeError, OSError):
        return {}
    if isinstance(data, dict):
        return data
    return {}


@app.on_event("startup")
def on_startup() -> None:
    update_encar_data()
    start_scheduler()


@app.on_event("shutdown")
def on_shutdown() -> None:
    stop_scheduler()


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    meta = _load_meta()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "meta": meta,
            "default_page_size": DEFAULT_PAGE_SIZE,
        },
    )


@app.get("/api/cars")
def get_cars(
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
) -> dict[str, Any]:
    cars = _load_cars_with_optional_limit()
    total = len(cars)
    start = (page - 1) * page_size
    end = start + page_size
    items = cars[start:end]
    has_more = end < total
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total_in_file": total,
        "has_more": has_more,
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/update-now")
def update_now() -> dict[str, str]:
    update_encar_data()
    return {"status": "update triggered"}
