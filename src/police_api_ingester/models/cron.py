from pydantic import BaseModel


class Cron(BaseModel):
    minute: str | None = None
    hour: str | None = None
    day_of_month: str | None = None
    month: str | None = None
    day_of_week: str | None = None
