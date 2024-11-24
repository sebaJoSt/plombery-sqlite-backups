import os
import tempfile
from datetime import datetime
from apscheduler.triggers.interval import IntervalTrigger
from plombery import task, get_logger, Trigger, register_pipeline
from plombery_sqlite_backups.helpers import append_subfolders_to_backupFolder, get_formatted_timestamp, backup_database_async, create_snapshot



INCREMENTAL_BACKUP_SOURCE_SQLITE_CONNECTION_STRING = os.path.expanduser("~/Documents/Loggbok/database.sqlite")
INCREMENTAL_BACKUP_TEMP_FOLDER =  os.path.join(tempfile.gettempdir(),"Loggbok")
INCREMENTAL_BACKUP_FOLDER = os.path.expanduser("~/Documents/Loggbok/database backups")
INCREMENTAL_BACKUP_SUBFOLDERS = {
  "ComputerName": True, 
  "DatabaseName": True, 
  "Year": True
}

@task
async def incremental_backup_sqlite_database():
    # using Plombery logger your logs will be stored
    # and accessible on the web UI
    logger = get_logger()

    logger.info(f"Source Database: {INCREMENTAL_BACKUP_SOURCE_SQLITE_CONNECTION_STRING}")

    logger.info("Starting the incremental backup")

    # Get the current time 
    now = datetime.now()

    logger.info("Create a temporary full backup")
   
    # Create temporary backup file name
    temp_backup_file_name = f"temp_{get_formatted_timestamp(now)}.sqlite"

    # Create a temporary full backup
    await backup_database_async(INCREMENTAL_BACKUP_SOURCE_SQLITE_CONNECTION_STRING,INCREMENTAL_BACKUP_TEMP_FOLDER, temp_backup_file_name)
    
    logger.info(f"Temporary full backup successfully created: {os.path.join(INCREMENTAL_BACKUP_TEMP_FOLDER, temp_backup_file_name)}")
    
    logger.info("Creating incremental backup (0% done)")

    # Create backup folder
    backup_folder = append_subfolders_to_backupFolder(INCREMENTAL_BACKUP_FOLDER, INCREMENTAL_BACKUP_SUBFOLDERS, now.strftime("%Y"), os.path.basename(INCREMENTAL_BACKUP_SOURCE_SQLITE_CONNECTION_STRING))
 
    # Backup folder/storage
    backup_storage_folder = os.path.join(backup_folder, "storage")

    # Create snapshot filename
    snapshot_file_name = f"snap_{get_formatted_timestamp(now)}.snapshot"

    # Snapshot filename full path
    snapshot_file_name_full_path = os.path.join(backup_folder, snapshot_file_name)

    # Create incremental backup
    await create_snapshot(os.path.join(INCREMENTAL_BACKUP_TEMP_FOLDER, temp_backup_file_name), backup_storage_folder, snapshot_file_name_full_path)

    # Delete temporary full backup
    os.remove(os.path.join(INCREMENTAL_BACKUP_TEMP_FOLDER,temp_backup_file_name))
    
    logger.info(f"Incremental backup successfully created: {os.path.join(backup_folder, snapshot_file_name)}")

register_pipeline(
    id="incremental_backup_pipeline",
    description="Creates an incremental backup of the given source sqlite database in a specified destination",
    tasks=[incremental_backup_sqlite_database],
    triggers=[
        Trigger(
            id="daily",
            name="Daily",
            description="Run the pipeline every day",
            schedule=IntervalTrigger(days=1),
        ),
    ],
)