from typer import Typer

from police_api_ingester.commands import ingest_commands, schedule_commands

app = Typer()

app.add_typer(
    ingest_commands,
    name="ingest",
    help="Commands to ingest data into the bronze database tables.",
)
app.add_typer(
    schedule_commands,
    name="schedule",
    help="Schedules the ingest of data into the bronze database tables.",
)
