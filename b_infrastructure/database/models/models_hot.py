# path: b_infrastructure/database/models/models_hot.py
from __future__ import annotations
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, DateTime, CHAR, CheckConstraint, Index, Integer
from b_infrastructure.database.base import HotBase

class LatestStatus(HotBase):
    __tablename__ = "latest_status"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    equip_code: Mapped[str] = mapped_column(String(30), unique=True, index=True)
    equip_status: Mapped[str] = mapped_column(CHAR(1), nullable=False)
    last_update: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    __table_args__ = (Index("ix_latest_status", "equip_code", "last_update"),)

class LatestInput(HotBase):
    __tablename__ = "latest_input"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    equip_code: Mapped[str] = mapped_column(String(30), unique=True, index=True)
    material_batch: Mapped[str] = mapped_column(String(30), nullable=False)
    feeding_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    __table_args__ = (Index("ix_latest_input", "material_batch", "feeding_time"),)