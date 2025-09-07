from sqlmodel import INTEGER, Column, Field, ForeignKey, SQLModel


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
    force_id: int = Field(
        sa_column=Column(
            "ForceId",
            ForeignKey("bronze.Force.Id"),
            nullable=False,
            primary_key=True,
            type_=INTEGER,
        )
    )
