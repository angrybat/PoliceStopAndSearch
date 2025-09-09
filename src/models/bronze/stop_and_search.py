from datetime import datetime as datetime_type
from typing import Any

from pydantic import model_validator
from sqlmodel import (
    BOOLEAN,
    DECIMAL,
    INTEGER,
    Column,
    DateTime,
    Field,
    ForeignKey,
    Relationship,
    SQLModel,
    String,
)

from src.models.bronze.force import Force


class StopAndSearch(SQLModel, table=True):
    __tablename__ = "StopAndSearch"
    __table_args__ = {"schema": "bronze"}

    id: int | None = Field(
        default=None,
        sa_column=Column("Id", primary_key=True, nullable=False, type_=INTEGER),
    )
    force_id: str | None = Field(
        default=None,
        sa_column=Column(
            "ForceId", ForeignKey("bronze.Force.Id"), nullable=False, type_=String(20)
        ),
    )
    type: str = Field(
        sa_column=Column("Type", nullable=False, unique=True, type_=String)
    )
    involved_person: bool = Field(
        sa_column=Column("InvolvedPerson", nullable=False, type_=BOOLEAN)
    )
    datetime: datetime_type = Field(
        sa_column=Column("Datetime", nullable=False, type_=DateTime(timezone=True))
    )
    operation: bool | None = Field(
        default=None, sa_column=Column("Operation", nullable=True, type_=BOOLEAN)
    )
    operation_name: str | None = Field(
        default=None, sa_column=Column("OperationName", nullable=True, type_=String)
    )
    latitude: float | None = Field(
        default=None, sa_column=Column("Latitude", nullable=True, type_=DECIMAL(9, 6))
    )
    longitude: float | None = Field(
        default=None, sa_column=Column("Longitude", nullable=True, type_=DECIMAL(9, 6))
    )
    street_id: int | None = Field(
        default=None, sa_column=Column("StreetId", nullable=True, type_=INTEGER)
    )
    street_name: str | None = Field(
        default=None, sa_column=Column("StreetName", nullable=True, type_=String)
    )
    gender: str | None = Field(
        default=None, sa_column=Column("Gender", nullable=True, type_=String)
    )
    age_range: str | None = Field(
        default=None, sa_column=Column("AgeRange", nullable=True, type_=String)
    )
    self_defined_ethnicity: str = Field(
        sa_column=Column("SelfDefinedEthnicity", nullable=False, type_=String)
    )
    officer_defined_ethnicity: str | None = Field(
        default=None,
        sa_column=Column("OfficerDefinedEthnicity", nullable=True, type_=String),
    )
    legislation: str | None = Field(
        default=None, sa_column=Column("Legislation", nullable=True, type_=String)
    )
    object_of_search: str | None = Field(
        default=None, sa_column=Column("ObjectOfSearch", nullable=True, type_=String)
    )
    outcome_name: str = Field(
        sa_column=Column("OutcomeName", nullable=False, type_=String),
    )
    outcome_id: str = Field(
        sa_column=Column("OutcomeId", nullable=False, type_=String),
    )
    outcome_linked_to_object_of_search: bool | None = Field(
        default=None,
        sa_column=Column("OutcomeLinkedToObjectOfSearch", nullable=True, type_=BOOLEAN),
    )
    removal_of_more_than_outer_clothing: bool | None = Field(
        default=None,
        sa_column=Column(
            "RemovalOfMoreThanOuterClothing", nullable=True, type_=BOOLEAN
        ),
    )

    force: Force = Relationship(back_populates="stop_and_searches")

    # SQLModel does not support aliases at the moment thus we need to use a validator
    @model_validator(mode="before")
    @classmethod
    def flatten(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Flatten nested location/outcome data into top-level fields."""
        location = values.get("location") or {}
        street = location.get("street") or {}
        outcome = values.get("outcome_object") or {}

        values["latitude"] = location.get("latitude")
        values["longitude"] = location.get("longitude")
        values["street_id"] = street.get("id")
        values["street_name"] = street.get("name")

        values["outcome_name"] = outcome.get("name")
        values["outcome_id"] = outcome.get("id")

        return values
