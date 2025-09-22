# path: b_infrastructure/database/models/models_cold.py
from __future__ import annotations
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, DateTime, Integer, UniqueConstraint
from b_infrastructure.database.base import ColdBase

class StatusHistory(ColdBase):
    __tablename__ = "history_status"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    equip_code: Mapped[str] = mapped_column(String(30), index=True)
    equip_status: Mapped[str] = mapped_column(String(5), nullable=False)
    create_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    __table_args__ = (UniqueConstraint("equip_code", "create_date", name="uix_status_history"),)

class InputHistory(ColdBase):
    __tablename__ = "history_input"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    equip_code: Mapped[str] = mapped_column(String(30), index=True)
    material_batch: Mapped[str] = mapped_column(String(100), nullable=False)
    feeding_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    create_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    end_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    __table_args__ = (UniqueConstraint("equip_code", "feeding_time", name="uix_input_history"),)

class StatusPeriod(ColdBase):
    __tablename__ = "history_status_period"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    equip_code: Mapped[str] = mapped_column(String(30), index=True)
    equip_status: Mapped[str] = mapped_column(String(5), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    __table_args__ = (UniqueConstraint("equip_code", "end_time", name="uix_status_period"),)

class InputPeriod(ColdBase):
    __tablename__ = "history_input_period"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    equip_code: Mapped[str] = mapped_column(String(30), index=True)
    material_batch: Mapped[str] = mapped_column(String(100), nullable=False)
    feeding_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    create_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    __table_args__ = (UniqueConstraint("equip_code", "feeding_time", name="uix_input_period"),)