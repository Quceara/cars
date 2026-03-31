from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, declarative_base, sessionmaker

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_URL = f"sqlite:///{(BASE_DIR / 'data' / 'app.db').as_posix()}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_SQLITE_URL)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


def init_db() -> None:
    from app import models  # noqa: F401

    Base.metadata.create_all(bind=engine)


def replace_cars_and_meta(cars: list[dict[str, Any]], meta: dict[str, Any]) -> None:
    from app.models import Car, SyncMeta

    init_db()
    with SessionLocal() as session:
        session.query(Car).delete()
        if cars:
            session.bulk_insert_mappings(
                Car,
                [
                    {
                        "brand": item.get("марка"),
                        "model": item.get("модель"),
                        "year": item.get("год"),
                        "mileage": item.get("пробег"),
                        "price": item.get("цена"),
                        "photo": item.get("фото"),
                    }
                    for item in cars
                ],
            )

        sync_meta = session.get(SyncMeta, 1)
        payload = json.dumps(meta, ensure_ascii=False)
        if sync_meta is None:
            sync_meta = SyncMeta(id=1, payload=payload)
            session.add(sync_meta)
        else:
            sync_meta.payload = payload
        session.commit()


def read_cars_page(page: int, page_size: int) -> tuple[list[dict[str, Any]], int]:
    from app.models import Car

    init_db()
    offset = (page - 1) * page_size
    with SessionLocal() as session:
        total = session.query(Car).count()
        rows = session.execute(select(Car).offset(offset).limit(page_size)).scalars().all()
    items = [
        {
            "марка": row.brand,
            "модель": row.model,
            "год": row.year,
            "пробег": row.mileage,
            "цена": row.price,
            "фото": row.photo,
        }
        for row in rows
    ]
    return items, total


def read_meta() -> dict[str, Any]:
    from app.models import SyncMeta

    init_db()
    with SessionLocal() as session:
        row = session.get(SyncMeta, 1)
    if not row:
        return {}
    try:
        data = json.loads(row.payload)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}
