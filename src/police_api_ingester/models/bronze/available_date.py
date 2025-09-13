from typing import TYPE_CHECKING, Any

from pydantic import model_validator
from sqlmodel import INTEGER, Column, Field, Relationship, SQLModel, String

from police_api_ingester.models.bronze.available_date_force_mapping import (
    AvailableDateForceMapping,
)

if TYPE_CHECKING:
    from .force import Force


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

    @property
    def force_ids(self) -> list[str]:
        return [force.id for force in self.forces if force.id is not None]

    # SQLModel does not support aliases at the moment thus we need to use a validator
    @model_validator(mode="before")
    @classmethod
    def set_year_month(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["year_month"] = values.get("date")
        return values

    def __eq__(self, other):
        if isinstance(other, AvailableDateWithForceIds):
            return other.year_month == self.year_month and set(other.force_ids) == set(
                self.force_ids
            )
        if isinstance(other, AvailableDate):
            return other.year_month == self.year_month
        return False

    def __str__(self):
        return self.year_month


# SQLModel does not support fields that are not database columns
class AvailableDateWithForceIds(SQLModel):
    year_month: str = Field()
    force_ids: list[str] = Field()

    # SQLModel does not support aliases at the moment thus we need to use a validator
    @model_validator(mode="before")
    @classmethod
    def set_year_month(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["year_month"] = values.get("date")
        values["force_ids"] = values.get("stop-and-search")
        return values

    def __eq__(self, other):
        if isinstance(other, AvailableDate):
            return other.year_month == self.year_month and set(self.force_ids) == set(
                other.force_ids
            )
        if isinstance(other, AvailableDateWithForceIds):
            return (
                other.year_month == self.year_month
                and self.force_ids == other.force_ids
            )
        return False
