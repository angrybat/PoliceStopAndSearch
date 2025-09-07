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
        sa_column=Column("Id", primary_key=True, nullable=False, type_=INTEGER)
    )
    force_id: int | None = Field(
        sa_column=Column(
            "ForceId", ForeignKey("bronze.Force.Id"), nullable=False, type_=INTEGER
        )
    )
    type: str = Field(
        sa_column=Column("Type", nullable=False, unique=True, type_=String)
    )
    involved_person: bool = Field(
        sa_column=Column("InvolvedPerson", nullable=False, type_=BOOLEAN)
    )
    datetime: str = Field(
        sa_column=Column("Datetime", nullable=False, type_=DateTime(timezone=True))
    )
    operation: bool | None = Field(
        sa_column=Column("Operation", nullable=True, type_=BOOLEAN)
    )
    operation_name: str | None = Field(
        sa_column=Column("OperationName", nullable=True, type_=String)
    )
    latitude: float | None = Field(
        sa_column=Column("Latitude", nullable=True, type_=DECIMAL(9, 6))
    )
    longitude: float | None = Field(
        sa_column=Column("Longitude", nullable=True, type_=DECIMAL(9, 6))
    )
    street_id: int | None = Field(
        sa_column=Column("StreetId", nullable=True, type_=INTEGER)
    )
    street_name: str | None = Field(
        sa_column=Column("StreetName", nullable=True, type_=String)
    )
    gender: str | None = Field(sa_column=Column("Gender", nullable=True, type_=String))
    age_range: str = Field(sa_column=Column("AgeRange", nullable=False, type_=String))
    self_defined_ethnicity: str = Field(
        sa_column=Column("SelfDefinedEthnicity", nullable=False, type_=String)
    )
    officer_defined_ethnicity: str = Field(
        sa_column=Column("OfficerDefinedEthnicity", nullable=False, type_=String)
    )
    legislation: str = Field(
        sa_column=Column("Legislation", nullable=False, type_=String)
    )
    object_of_search: str = Field(
        sa_column=Column("ObjectOfSearch", nullable=False, type_=String)
    )
    outcome_name: str = Field(
        sa_column=Column("OutcomeName", nullable=False, type_=String)
    )
    outcome_id: str = Field(sa_column=Column("OutcomeId", nullable=False, type_=String))
    outcome_linked_to_object_of_search: bool | None = Field(
        sa_column=Column("OutcomeLinkedToObjectOfSearch", nullable=True, type_=BOOLEAN)
    )
    removal_of_more_than_outer_clothing: bool | None = Field(
        sa_column=Column("RemovalOfMoreThanOuterClothing", nullable=True, type_=BOOLEAN)
    )

    force: Force = Relationship(back_populates="stop_and_searches")
