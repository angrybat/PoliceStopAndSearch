from typing import TYPE_CHECKING

from sqlmodel import Column, Field, Relationship, SQLModel, String

from src.models.bronze.available_date_force_mapping import AvailableDateForceMapping

if TYPE_CHECKING:
    from src.models.bronze.force import Force


class AvailableDate(SQLModel, table=True):
    __tablename__ = "AvailableDate"
    __table_args__ = {"schema": "bronze"}

    id: int | None = Field(sa_column=Column(
        "Id", nullable=False, primary_key=True))
    year_month: str = Field(sa_column=Column(
        "YearMonth", type_=String(7), nullable=False, unique=True))

    forces: list["Force"] = Relationship(
        back_populates="available_dates", link_model=AvailableDateForceMapping)
