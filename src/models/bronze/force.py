from sqlmodel import Column, Field, SQLModel


class Force(SQLModel, table=True):
    __tablename__ = "Force"
    __table_args__ = {"schema": "bronze"}

    id: int | None = Field(sa_column=Column(
        "Id", primary_key=True, nullable=False))
    name: str = Field(sa_column=Column("Name", nullable=False))
    api_id: str = Field(sa_column=Column("ApiId", nullable=False, unique=True))
