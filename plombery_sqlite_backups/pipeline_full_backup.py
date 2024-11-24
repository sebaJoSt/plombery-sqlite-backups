import asyncio
import os
from pydantic import BaseModel
from datetime import datetime
from apscheduler.triggers.interval import IntervalTrigger
from plombery import task, get_logger, Trigger, register_pipeline

from plombery_sqlite_backups.helpers import append_subfolders_to_backupFolder, get_formatted_timestamp, backup_database_async, compress_file_lz4

FULL_BACKUP_SQLITE_PATH = os.path.expanduser("~/Documents/Loggbok/database.sqlite")

class FullBackupInputParams(BaseModel):
    sqlite_path: str = FULL_BACKUP_SQLITE_PATH
    convert_currency: bool = False

FULL_BACKUP_FOLDER =  os.path.expanduser("~/Documents/Loggbok/database backups")
FULL_BACKUP_SUBFOLDERS = {
  "ComputerName": True, 
  "DatabaseName": True, 
  "Year": True
}

@task
async def full_compressed_backup_sqlite_database(params: FullBackupInputParams):
    # using Plombery logger your logs will be stored
    # and accessible on the web UI
    logger = get_logger()
    
    if not os.path.exists(params.sqlite_path):
        logger.error(f"Could not find source database: {params.sqlite_path}")
        return

    logger.info(f"Source Database: {params.sqlite_path}")
 
    # Get the current time 
    now = datetime.now()
   
    # Create backup folder
    backup_folder = append_subfolders_to_backupFolder(FULL_BACKUP_FOLDER, FULL_BACKUP_SUBFOLDERS, now.strftime("%Y"), os.path.basename(params.sqlite_path))

    # Create backup file name
    backup_file_name = f"backup_{get_formatted_timestamp(now)}.sqlite"

    logger.info(f"Starting the full backup into {backup_folder}")

    # Create a full compressed backup
    await backup_database_async(params.sqlite_path, backup_folder, backup_file_name)

    logger.info(f"Backup finished")
    logger.info(f"Starting compression (LZ4) ...")

    # Compress the file in format lz4
    await asyncio.to_thread(compress_file_lz4, os.path.join(backup_folder, backup_file_name), os.path.join(backup_folder, backup_file_name) + '.lz4', True )

    logger.info(f"Full compressed (LZ4) backup successfully created: {os.path.join(backup_folder, backup_file_name)}.lz4")

register_pipeline(
    id="full_backup_pipeline",
    description="Creates a full compressed backup (LZ4) of the given source sqlite database in a specified destination",
    tasks=[full_compressed_backup_sqlite_database],
    params=FullBackupInputParams,
    triggers=[
        Trigger(
            id="daily",
            name="Daily",
            description="Run the pipeline every day",
            schedule=IntervalTrigger(days=1),
            params={
                "sqlite_path": FULL_BACKUP_SQLITE_PATH,
                "convert_currency": True,
            },
        )
    ],
)