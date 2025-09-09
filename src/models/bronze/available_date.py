from typing import TYPE_CHECKING, Any

from pydantic import model_validator
from sqlmodel import INTEGER, Column, Field, Relationship, SQLModel, String

from src.models.bronze.available_date_force_mapping import AvailableDateForceMapping

if TYPE_CHECKING:
    from src.models.bronze.force import Force


class AvailableDate(SQLModel, table=True):
    __tablename__ = "AvailableDate"
    __table_args__ = {"schema": "bronze"}

    id: int | None = Field(
        default=None,
        sa_column=Column("Id", nullable=False, primary_key=True, type_=INTEGER),
    )
    year_month: str = Field(
        sa_column=Column("YearMonth", type_=String(7), nullable=False, unique=True),
    )

    forces: list["Force"] = Relationship(
        back_populates="available_dates", link_model=AvailableDateForceMapping
    )

    # SQLModel does not support aliases at the moment thus we need to use a validator
    @model_validator(mode="before")
    @classmethod
    def set_month_year(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["year_month"] = values.get("date")
        return values
