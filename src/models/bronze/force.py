from typing import TYPE_CHECKING

from sqlmodel import Column, Field, Relationship, SQLModel, String

from src.models.bronze.available_date_force_mapping import AvailableDateForceMapping

if TYPE_CHECKING:
    from src.models.bronze.available_date import AvailableDate
    from src.models.bronze.stop_and_search import StopAndSearch


class Force(SQLModel, table=True):
    __tablename__ = "Force"
    __table_args__ = {"schema": "bronze"}

    id: str | None = Field(
        sa_column=Column("Id", primary_key=True, nullable=False, type_=String(20)),
    )
    name: str = Field(sa_column=Column("Name", nullable=False, type_=String))

    stop_and_searches: list["StopAndSearch"] = Relationship(back_populates="force")
    available_dates: list["AvailableDate"] = Relationship(
        back_populates="forces", link_model=AvailableDateForceMapping
    )
