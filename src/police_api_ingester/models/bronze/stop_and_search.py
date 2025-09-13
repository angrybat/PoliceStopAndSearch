from datetime import datetime as datetime_type
from decimal import Decimal
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

from police_api_ingester.models.bronze.force import Force


class StopAndSearch(SQLModel, table=True):
    __tablename__ = "StopAndSearch"
    __table_args__ = {"schema": "bronze"}

    id: int | None = Field(
        default=None,
        sa_column=Column(
            "Id",
            INTEGER,
            primary_key=True,
            nullable=False,
        ),
    )
    force_id: str | None = Field(
        default=None,
        sa_column=Column(
            "ForceId", String(20), ForeignKey("bronze.Force.Id"), nullable=False
        ),
    )
    type: str = Field(sa_column=Column("Type", String, nullable=False))
    involved_person: bool = Field(
        sa_column=Column("InvolvedPerson", BOOLEAN, nullable=False)
    )
    datetime: datetime_type = Field(
        sa_column=Column("Datetime", DateTime(timezone=True), nullable=False)
    )
    operation: bool | None = Field(
        default=None, sa_column=Column("Operation", BOOLEAN, nullable=True)
    )
    operation_name: str | None = Field(
        default=None, sa_column=Column("OperationName", String, nullable=True)
    )
    latitude: Decimal | None = Field(
        default=None, sa_column=Column("Latitude", DECIMAL(9, 6), nullable=True)
    )
    longitude: Decimal | None = Field(
        default=None, sa_column=Column("Longitude", DECIMAL(9, 6), nullable=True)
    )
    street_id: int | None = Field(
        default=None, sa_column=Column("StreetId", INTEGER, nullable=True)
    )
    street_name: str | None = Field(
        default=None, sa_column=Column("StreetName", String, nullable=True)
    )
    gender: str | None = Field(
        default=None, sa_column=Column("Gender", String, nullable=True)
    )
    age_range: str | None = Field(
        default=None, sa_column=Column("AgeRange", String, nullable=True)
    )
    self_defined_ethnicity: str | None = Field(
        default=None, sa_column=Column("SelfDefinedEthnicity", String, nullable=True)
    )
    officer_defined_ethnicity: str | None = Field(
        default=None, sa_column=Column("OfficerDefinedEthnicity", String, nullable=True)
    )
    legislation: str | None = Field(
        default=None, sa_column=Column("Legislation", String, nullable=True)
    )
    object_of_search: str | None = Field(
        default=None, sa_column=Column("ObjectOfSearch", String, nullable=True)
    )
    outcome_name: str = Field(
        sa_column=Column("OutcomeName", String, nullable=False),
    )
    outcome_id: str = Field(
        sa_column=Column("OutcomeId", String, nullable=False),
    )
    outcome_linked_to_object_of_search: bool | None = Field(
        default=None,
        sa_column=Column("OutcomeLinkedToObjectOfSearch", BOOLEAN, nullable=True),
    )
    removal_of_more_than_outer_clothing: bool | None = Field(
        default=None,
        sa_column=Column("RemovalOfMoreThanOuterClothing", BOOLEAN, nullable=True),
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
