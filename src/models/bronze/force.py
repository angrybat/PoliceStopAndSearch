from typing import TYPE_CHECKING

from sqlmodel import Column, Field, Relationship, SQLModel

from src.models.bronze.available_date_force_mapping import AvailableDateForceMapping

if TYPE_CHECKING:
    from src.models.bronze.available_date import AvailableDate
    from src.models.bronze.stop_and_search import StopAndSearch


class Force(SQLModel, table=True):
    __tablename__ = "Force"
    __table_args__ = {"schema": "bronze"}

    id: int | None = Field(sa_column=Column(
        "Id", primary_key=True, nullable=False))
    name: str = Field(sa_column=Column("Name", nullable=False))
    api_id: str = Field(sa_column=Column("ApiId", nullable=False, unique=True))

    stop_and_searches: list["StopAndSearch"] = Relationship(
        back_populates="force")
    available_dates: list["AvailableDate"] = Relationship(
        back_populates="forces", link_model=AvailableDateForceMapping)
