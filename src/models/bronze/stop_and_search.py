from sqlmodel import DECIMAL, Column, Field, Relationship, SQLModel

from src.models.bronze.force import Force


class StopAndSearch(SQLModel, table=True):
    __tablename__ = "StopAndSearch"
    __table_args__ = {"schema": "bronze"}

    id: int | None = Field(sa_column=Column(
        "Id", primary_key=True, nullable=False))
    force_id: int = Field("ForceId", sa_column=Column(
        nullable=False, foreign_key="bronze.Force.Id"))
    type: str = Field("Type", sa_column=Column(nullable=False, unique=True))
    involved_person: bool = Field(
        "InvolvedPerson", sa_column=Column(nullable=False))
    datetime: str = Field("Datetime", sa_column=Column(nullable=False))
    operation: bool | None = Field(
        "Operation", sa_column=Column(nullable=True))
    operation_name: str | None = Field(
        "OperationName", sa_column=Column(nullable=True))
    latitude: float | None = Field(
        "Latitude", sa_column=Column(nullable=True, type_=DECIMAL(9, 6)))
    longitude: float | None = Field(
        "Longitude", sa_column=Column(nullable=True, type_=DECIMAL(9, 6)))
    street_id: int | None = Field(
        "StreetId", sa_column=Column(nullable=True))
    street_name: str | None = Field(
        "StreetName", sa_column=Column(nullable=True))
    gender: str | None = Field("Gender", sa_column=Column(nullable=True))
    age_range: str = Field("AgeRange", sa_column=Column(nullable=False))
    self_defined_ethnicity: str = Field(
        "SelfDefinedEthnicity", sa_column=Column(nullable=False))
    officer_defined_ethnicity: str = Field(
        "OfficerDefinedEthnicity", sa_column=Column(nullable=False))
    legislation: str = Field("Legislation", sa_column=Column(nullable=False))
    object_of_search: str = Field(
        "ObjectOfSearch", sa_column=Column(nullable=False))
    outcome: str = Field("Outcome", sa_column=Column(nullable=False))
    outcome_linked_to_object_of_search: bool | None = Field(
        "OutcomeLinkedToObjectOfSearch", sa_column=Column(nullable=True))
    removal_of_more_than_outer_clothing: bool | None = Field(
        "RemovalOfMoreThanOuterClothing", sa_column=Column(nullable=True))

    force: Force = Relationship(back_populates="stop_and_searches")
