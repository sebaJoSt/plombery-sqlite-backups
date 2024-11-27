import os
import click
import uvicorn
from plombery import _Plombery
from plombery_sqlite_backups import pipeline_full_backup

@click.command()
@click.option("--backupDir", "-b", default=os.path.join(os.path.expanduser("~"), "Documents/Loggbok/database backups/"),type=click.Path(exists=True), required=False, help="Backup directory")
@click.option("--sqliteFile", "-s", default=os.path.join(os.path.expanduser("~"), "Documents/Loggbok/database.sqlite"),type=click.Path(exists=True), required=False, help="Path to SQLite database file")
def cli(backupdir: str, sqlitefile: str) -> None:
    
    pipeline_full_backup.init(backupdir, sqlitefile)
    
    app = _Plombery()
    app.register_pipeline(pipeline_full_backup.create_pipeline())
    
    uvicorn.run(app)
    
if __name__ == "__main__":
    cli()

