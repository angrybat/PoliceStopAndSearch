from typer import Typer

from .commands import ingest_commands

app = Typer()

app.add_typer(ingest_commands, name="ingest")

if __name__ == "__main__":
    app()
