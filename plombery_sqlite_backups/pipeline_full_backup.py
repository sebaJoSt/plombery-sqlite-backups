import asyncio
import os
from pydantic import BaseModel
from datetime import datetime
from apscheduler.triggers.interval import IntervalTrigger
from plombery import task, get_logger, Trigger, Pipeline
from plombery_sqlite_backups.helpers import (
    append_subfolders_to_backupFolder,
    get_formatted_timestamp,
    backup_database_async,
    compress_file_lz4,
)

# from command line interface - Assigned in init function, which is called from app:cli
backup_dir = str
sqlite_file = str


class InputParams(BaseModel):
    lz4_compressed: bool = False


BACKUP_DIR_SUBFOLDERS = {"Year": True, "ComputerName": True, "DatabaseName": True}


def init(backupdir, sqlitefile):
    global backup_dir
    backup_dir = backupdir
    global sqlite_file
    sqlite_file = sqlitefile


@task
async def full_backup_sqlite_database(params: InputParams):

    logger = get_logger()

    if not os.path.exists(sqlite_file):
        raise Exception(f"Could not find source database: {sqlite_file}")

    logger.info(f"Source Database: {sqlite_file}")

    if params.lz4_compressed:
        logger.info("Mode: LZ4 Compression")
    else:
        logger.info("Mode: Uncompressed")

    # Get the current time
    now = datetime.now()

    # Create backup folder
    backup_folder = append_subfolders_to_backupFolder(
        backup_dir,
        BACKUP_DIR_SUBFOLDERS,
        now.strftime("%Y"),
        os.path.basename(sqlite_file),
        isFullBackup=True,
    )

    # Create backup file name
    backup_file_name = f"backup_{get_formatted_timestamp(now)}.sqlite"

    logger.info(f"Starting the full backup into {backup_folder}")

    # Create a full backup
    await backup_database_async(sqlite_file, backup_folder, backup_file_name)

    if params.lz4_compressed:
        # Compress the file in format lz4
        logger.info("Backup created")
        logger.info("Starting compression (LZ4) ...")
        await asyncio.to_thread(
            compress_file_lz4,
            os.path.join(backup_folder, backup_file_name),
            os.path.join(backup_folder, backup_file_name) + ".lz4",
            True,
        )
        logger.info(
            f"Full compressed (LZ4) backup successfully created: {os.path.join(backup_folder, backup_file_name)}.lz4"
        )
    else:
        # Do not compress the file - finished
        logger.info(
            f"Full backup successfully created: {os.path.join(backup_folder, backup_file_name)}"
        )


def create_pipeline() -> Pipeline:
    return Pipeline(
        id="full_backup_pipeline",
        name="Full Backup Pipeline",
        description="Creates a full compressed backup (LZ4)",
        tasks=[full_backup_sqlite_database],
        params=InputParams,
        triggers=[
            Trigger(
                id="weekly",
                name="Weekly",
                description="Run the pipeline every week between 10pm and 11pm",
                schedule=IntervalTrigger(
                    start_date="2000-01-01 22:00:00", weeks=1, jitter=3600
                ),
                params=InputParams(lz4_compressed=True),
            )
        ],
    )
