import os
import click
import uvicorn
from plombery import _Plombery
from plombery_sqlite_backups import pipeline_full_backup
from plombery_sqlite_backups import pipeline_inc_backup


@click.command()
@click.option(
    "--skipFullBackups",
    is_flag=True,
    help="Disables Full Backup Pipeline",
)
@click.option(
    "--backupDir",
    "-b",
    default=os.path.join(
        os.path.expanduser("~"),
        "Documents/Loggbok/database backups/",
    ),
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True),
    required=False,
    help="Backup directory (subfolder structure is generated when backups are initiated)",
)
@click.option(
    "--sqliteFile",
    "-s",
    default=os.path.join(os.path.expanduser("~"), "Documents/Loggbok/database.sqlite"),
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    required=False,
    help="Path to SQLite database file",
)
def cli(skipfullbackups: bool, backupdir: str, sqlitefile: str) -> None:

    pipeline_full_backup.init(backupdir, sqlitefile)
    pipeline_inc_backup.init(backupdir, sqlitefile)

    app = _Plombery()
    app.register_pipeline(pipeline_inc_backup.create_pipeline())
    if not skipfullbackups:
        app.register_pipeline(pipeline_full_backup.create_pipeline())

    uvicorn.run(app)


if __name__ == "__main__":
    cli()
