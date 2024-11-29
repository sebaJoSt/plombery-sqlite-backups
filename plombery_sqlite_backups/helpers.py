import asyncio
import os
import socket
import sqlite3
import aiosqlite
import tzlocal
import lz4.frame


async def backup_database_async(
    sqlite_file, backup_folder, backup_file_name, use_vacuum_into=True
):
    """
    Asynchronously backs up a SQLite database to a specified folder.

    Args:
        sqlite_file (str): The sqlite source file
        backup_folder (str): The destination folder where the backup will be saved.
        backup_file_name (str): The name of the backup file.
        use_vacuum_into (bool): True: Uses VACUUM INTO
                                False: Uses SQLite Backup API

    """

    # Construct the full path of the backup file
    backup_file_name_full_path = os.path.join(backup_folder, backup_file_name)

    # Create the backup folder if it doesn't exist
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

    # Perform the database backup
    if use_vacuum_into:
        # VACUUM INTO
        await asyncio.to_thread(
            execute_sqlite_command,
            sqlite_file,
            f"VACUUM INTO '{backup_file_name_full_path}'",
        )
    else:
        # SQLite Backup API
        await backup_sqlite_db(sqlite_file, backup_file_name_full_path)


async def backup_sqlite_db(sqlite_file, backup_file_name_full_path):
    # SQLite Backup API
    async with aiosqlite.connect(sqlite_file) as conn1:
        async with aiosqlite.connect(backup_file_name_full_path) as conn2:
            await conn1.backup(conn2)


def compress_file_lz4(source, destination, deleteSourceAfter):
    with open(source, "rb") as infile:
        with open(destination, "wb") as outfile:
            outfile.write(lz4.frame.compress(infile.read()))

    if deleteSourceAfter:
        os.remove(source)


def execute_sqlite_command(sqlite_file, sqlite_command):

    # Connect to the SQLite database
    with sqlite3.connect(sqlite_file) as conn:
        # Create a cursor object
        cursor = conn.cursor()

        # Execute the SQLite command
        cursor.execute(sqlite_command)

        # Commit the changes
        conn.commit()


def get_sqlite_page_size(db_file):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        return page_size


def get_sqlite_page_count(db_file):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        return page_count


def get_formatted_timestamp(time):
    # Get the current local timezone
    local_tz = tzlocal.get_localzone()

    # Replace the tzinfo
    time = time.replace(tzinfo=local_tz)

    # Format the time with the timezone suffix and replace the : with .
    timestamp = time.isoformat(timespec="milliseconds")
    timestamp = timestamp.replace(":", ".")

    return timestamp


def append_subfolders_to_backupFolder(
    backupfolder, subfoldersDict, year, sqliteDatabaseName, isFullBackup
):
    """
    Appends the subfolders to the backup folder

    Args:
        backupfolder (str): the backup folder
        subfoldersDict (str, bool ): a dictionary containing the subfolders to add
        year (str): the year part of the snapshot time
        sqliteDatabaseName (str): the sqlite database file name

    Returns:
        str: the backup folder including the appended sub folders
    """

    if subfoldersDict["Year"]:
        backupfolder = os.path.join(backupfolder, year)
    if subfoldersDict["ComputerName"]:
        backupfolder = os.path.join(backupfolder, socket.gethostname())
    if subfoldersDict["DatabaseName"]:
        backupfolder = os.path.join(backupfolder, sqliteDatabaseName)
    if isFullBackup:
        backupfolder = os.path.join(backupfolder, "full backups")
    else:
        backupfolder = os.path.join(backupfolder, "incremental backups")
    return backupfolder
