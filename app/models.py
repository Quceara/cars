from __future__ import annotations

from sqlalchemy import Float, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Car(Base):
    __tablename__ = "cars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    brand: Mapped[str | None] = mapped_column(Text, nullable=True)
    model: Mapped[str | None] = mapped_column(Text, nullable=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mileage: Mapped[float | None] = mapped_column(Float, nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    photo: Mapped[str | None] = mapped_column(Text, nullable=True)


class SyncMeta(Base):
    __tablename__ = "sync_meta"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
