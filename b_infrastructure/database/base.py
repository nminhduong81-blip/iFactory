from __future__ import annotations
from sqlalchemy.orm import DeclarativeBase

class HotBase(DeclarativeBase):
    pass

class ColdBase(DeclarativeBase):
    pass

__all__ = ["HotBase", "ColdBase"]