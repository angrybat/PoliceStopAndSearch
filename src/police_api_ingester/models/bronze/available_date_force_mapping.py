from sqlmodel import INTEGER, Column, Field, ForeignKey, SQLModel, String


class AvailableDateForceMapping(SQLModel, table=True):
    __tablename__ = "AvailableDateForceMapping"
    __table_args__ = {"schema": "bronze"}

    available_date_id: int = Field(
        sa_column=Column(
            "AvailableDateId",
            ForeignKey("bronze.AvailableDate.Id"),
            nullable=False,
            primary_key=True,
            type_=INTEGER,
        )
    )
    force_id: str = Field(
        sa_column=Column(
            "ForceId",
            ForeignKey("bronze.Force.Id"),
            nullable=False,
            primary_key=True,
            type_=String(20),
        )
    )
