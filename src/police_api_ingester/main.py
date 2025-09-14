from typer import Typer

from police_api_ingester.commands import ingest_commands, schedule_commands

app = Typer()

app.add_typer(ingest_commands, name="ingest")
