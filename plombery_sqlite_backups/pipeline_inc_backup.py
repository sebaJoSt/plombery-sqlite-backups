import asyncio
import hashlib
import os
import tempfile
from datetime import datetime
import aiofiles
from apscheduler.triggers.interval import IntervalTrigger
from plombery import task, get_logger, Trigger, Pipeline
from pydantic import BaseModel
from plombery_sqlite_backups.helpers import (
    append_subfolders_to_backupFolder,
    get_formatted_timestamp,
    backup_database_async,
    get_sqlite_page_count,
    get_sqlite_page_size,
)

# from command line interface - Assigned in init function, which is called from app:cli
backup_dir = str
sqlite_file = str


class InputParams(BaseModel):
    scheduled_do_not_change_this: bool = False


def init(backupdir, sqlitefile):
    global backup_dir
    backup_dir = backupdir
    global sqlite_file
    sqlite_file = sqlitefile


TEMP_FOLDER = os.path.join(
    tempfile.gettempdir(), "plombery_sqlite_backups", "pipeline_inc_backup"
)
BACKUP_DIR_SUBFOLDERS = {"Year": True, "ComputerName": True, "DatabaseName": True}


@task
async def incremental_backup_sqlite_database(params: InputParams):
    # using Plombery logger your logs will be stored
    # and accessible on the web UI
    logger = get_logger()

    if not os.path.exists(sqlite_file):
        raise Exception(f"Could not find source database: {sqlite_file}")

    logger.info(f"Source Database: {sqlite_file}")

    # Get the current time
    now = datetime.now()

    # Create backup folder
    backup_folder = append_subfolders_to_backupFolder(
        backup_dir,
        BACKUP_DIR_SUBFOLDERS,
        now.strftime("%Y"),
        os.path.basename(sqlite_file),
        isFullBackup=False,
    )

    logger.info(f"Starting the incremental backup into {backup_folder}")
    logger.info("Creating temporary full backup ..")

    # Create temporary backup file name
    temp_backup_file_name = f"temp_{get_formatted_timestamp(now)}.sqlite"

    # Create a temporary full backup with SQLite Backup API
    await backup_database_async(
        sqlite_file, TEMP_FOLDER, temp_backup_file_name, use_vacuum_into=False
    )

    logger.info(
        f"Temporary full backup file successfully created: {os.path.join(TEMP_FOLDER, temp_backup_file_name)}"
    )

    logger.info("Creating incremental backup ..")

    # Backup folder/storage
    backup_storage_folder = os.path.join(backup_folder, "storage")

    # Create snapshot filename
    if params.scheduled_do_not_change_this:
        snapshot_file_name = f"snap_{get_formatted_timestamp(now)}.snapshot"
    else:
        snapshot_file_name = f"manual_{get_formatted_timestamp(now)}.snapshot"

    # Snapshot filename full path
    snapshot_file_name_full_path = os.path.join(backup_folder, snapshot_file_name)

    # Create incremental backup
    pages_written_count, page_total_count, page_size = await create_snapshot(
        os.path.join(TEMP_FOLDER, temp_backup_file_name),
        backup_storage_folder,
        snapshot_file_name_full_path,
    )

    # Delete temporary full backup
    logger.info("Deleting temporary file ..")
    os.remove(os.path.join(TEMP_FOLDER, temp_backup_file_name))

    logger.info("Temporary file successfully deleted")

    # Statistics
    percentage_written = round(pages_written_count * 100 / page_total_count)
    megabyte_added_storage = round(page_size * pages_written_count / 1048576, 3)
    megabyte_snapshot_file = round(
        os.path.getsize(snapshot_file_name_full_path) / 1048576, 3
    )
    logger.info(
        f"{pages_written_count} of {page_total_count} Snapshot Pages ({percentage_written}%) written to 'storage' folder"
    )
    if pages_written_count != page_total_count:
        logger.info(
            "If the percentage of written pages is below 100%, but you would expect it to be 100% (first incremental backup in empty 'storage' folder), this is likely due to the presence of duplicate pages in the SQLite database"
        )
    logger.info(
        f"{megabyte_added_storage} MB added to 'storage' + Snapshot file {megabyte_snapshot_file} MB = Total {round(megabyte_added_storage + megabyte_snapshot_file,3)} MB"
    )

    logger.info("All done.")


async def create_snapshot(
    source_database_full_path, backup_storage_full_path, backup_file_name_full_path
) -> tuple[int, int]:
    """
    Creates an incremental snapshot

    Args:
        source_database_full_path (str): Path to the source database file
        backup_storage_full_path (str): the destination "storage" folder, where the hashed pages are written to
        backup_file_name_full_path (str): the destination snapshot file

    Returns:
        tuple[int, int, int]: 1: the number of written pages
                              2: total pages
                              3: page size
    """
    logger = get_logger()

    # Get the page size of the SQLite database
    page_size = get_sqlite_page_size(source_database_full_path)

    # Get the page count of the SQLite database
    page_count = get_sqlite_page_count(source_database_full_path)

    # Variables to calculate % done
    current_page = 0
    page_current_step = 0.1 * page_count
    pages_done_percent = 0
    pages_written_count = 0

    logger.info("Writing to 'storage' folder .. (0% done)")

    file_names = []
    async with aiofiles.open(source_database_full_path, "rb") as db_file:
        async for page_content in read_page_from_temporary_sqlite_file(
            db_file, page_size
        ):
            # Create hash from page content
            hash_object = hashlib.sha256(page_content)
            hash_string = hash_object.hexdigest()

            # Write hashed page into storage folder
            written_successfully = await write_hashed_page__into_storage(
                page_content, backup_storage_full_path, hash_string
            )
            if written_successfully:
                pages_written_count += 1

            # Add hashed string to list
            file_names.append(
                os.path.join(backup_storage_full_path, hash_string[0], hash_string[1:])
            )

            # Calculate and log % done
            current_page += 1

            pages_done_percent = round(current_page * 100 / page_count)
            if current_page >= page_current_step:
                await asyncio.sleep(0.2)
                if current_page != page_count:
                    logger.info(
                        f"Writing to 'storage' folder .. ({pages_done_percent:.0f}% done)"
                    )
                page_current_step += 0.1 * page_count
    logger.info("Writing to 'storage' folder finished (100% done)")

    # Write snapshot file
    logger.info("Creating the snapshot file ..")
    async with aiofiles.open(backup_file_name_full_path, "w") as snapshot_file:
        for file_name in file_names:
            await snapshot_file.write(file_name + "\n")
    logger.info(f"Snapshot file successfully created: {backup_file_name_full_path}")
    return pages_written_count, page_count, page_size


async def read_page_from_temporary_sqlite_file(db_file, page_size):
    page_content = bytearray(page_size)
    while True:
        bytes_read = await db_file.readinto(page_content)
        if bytes_read == 0:
            break
        yield page_content[:bytes_read]


async def write_hashed_page__into_storage(
    page_content, backup_storage_full_path, hash_string
) -> bool:
    """
    Asynchronously writes hashed page into storage folder (if it's not already there)

    Returns:
        bool: True:  written successfuly
              False: file already existed
    """

    file_dir = os.path.join(backup_storage_full_path, hash_string[0])
    file_name = hash_string[1:]
    file_dest = os.path.join(file_dir, file_name)

    if not os.path.exists(file_dest):
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        async with aiofiles.open(file_dest, "wb") as obj_file:
            await obj_file.write(page_content)

        return True
    else:
        return False


def create_pipeline() -> Pipeline:
    return Pipeline(
        id="incremental_backup_pipeline",
        name="Incremental Backup Pipeline",
        params=InputParams,
        description="Creates an incremental backup",
        tasks=[incremental_backup_sqlite_database],
        triggers=[
            Trigger(
                id="daily",
                name="Daily",
                description="Run the pipeline every day between 6pm and 9pm",
                schedule=IntervalTrigger(
                    start_date="2000-01-01 18:00:00", days=1, jitter=10800
                ),
                params=InputParams(scheduled_do_not_change_this=True),
            )
        ],
    )
