import asyncio
import os
import socket
import sqlite3
import tzlocal
import lz4.frame
import hashlib
import aiofiles
from plombery import get_logger

async def backup_database_async(sqlite_connect_string, backup_folder, backup_file_name): 
    """
    Asynchronously backs up a SQLite database to a specified folder.

    Args:
        backup_folder (str): The folder where the backup will be saved.
        backup_file_name (str): The name of the backup file.

    Returns:
        str: The file name of the backup without the extension.
    """

    # Construct the full path of the backup file
    backup_file_name_full_path = os.path.join(backup_folder, backup_file_name)

    # Create the backup folder if it doesn't exist
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

    # Perform the database backup
    await asyncio.to_thread(execute_sqlite_command, sqlite_connect_string, f"VACUUM INTO '{backup_file_name_full_path}'")
  
def compress_file_lz4(source, destination, deleteSourceAfter):
    with open(source, 'rb') as infile:
        with open(destination, 'wb') as outfile:
            outfile.write(lz4.frame.compress(infile.read()))
        
    if deleteSourceAfter:
        os.remove(source)

def execute_sqlite_command(sqlite_connectstring, sqlite_command):

    # Connect to the SQLite database
    with sqlite3.connect(sqlite_connectstring) as conn:
        # Create a cursor object
        cursor = conn.cursor()

        # Execute the SQLite command
        cursor.execute(sqlite_command)

        # Commit the changes
        conn.commit()

def get_sqlite_page_size(db_file):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute('PRAGMA page_size')
        page_size = cursor.fetchone()[0]
        return page_size

def get_sqlite_page_count(db_file):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute('PRAGMA page_count')
        page_count = cursor.fetchone()[0]
        return page_count

def get_formatted_timestamp(time):
     # Get the current local timezone
    local_tz = tzlocal.get_localzone()

    # Replace the tzinfo
    time = time.replace(tzinfo=local_tz)

    # Format the time with the timezone suffix and replace the : with .
    timestamp = time.strftime('%Y-%m-%dT%H:%M:%S.%f%z')    
    timestamp = timestamp.replace(':', '.')

    return timestamp

def append_subfolders_to_backupFolder(backupfolder, subfoldersDict, year, sqliteDatabaseName):
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
    
    if subfoldersDict["ComputerName"]:
          backupfolder = os.path.join(backupfolder, socket.gethostname())
    if subfoldersDict["DatabaseName"]:
          backupfolder = os.path.join(backupfolder, sqliteDatabaseName)
    if subfoldersDict["Year"]:
          backupfolder = os.path.join(backupfolder, year)
          
    return backupfolder

async def read_page_from_temporary_sqlite_file(db_file, page_size):
    page_content = bytearray(page_size)
    while True:
        bytes_read = await db_file.readinto(page_content)
        if bytes_read == 0:
            break
        yield page_content[:bytes_read]

async def write_hashed_page__into_storage(page_content, backup_storage_full_path, hash_string):
    file_dir = os.path.join(backup_storage_full_path, hash_string[0])
    file_name = hash_string[1:]
    file_dest = os.path.join(file_dir, file_name)

    if not os.path.exists(file_dest):
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        async with aiofiles.open(file_dest, "wb") as obj_file:
            await obj_file.write(page_content)

async def create_snapshot(temp_database_backup_full_path, backup_storage_full_path, backup_file_name_full_path):

    logger = get_logger()

    # Get the page size of the SQLite database
    page_size = get_sqlite_page_size(temp_database_backup_full_path)

    # Get the page count of the SQLite database
    page_count = get_sqlite_page_count(temp_database_backup_full_path)

    # Variables to calculate % done
    current_page = 0    
    page_current_step = 0.1 * page_count
    pages_done_percent = 0

    file_names = []
    async with aiofiles.open(temp_database_backup_full_path, "rb") as db_file:
        async for page_content in read_page_from_temporary_sqlite_file(db_file, page_size):
            # Create hash from page content
            hash_object = hashlib.sha256(page_content)
            hash_string = hash_object.hexdigest()
            
            # Write hashed page into storage folder 
            await write_hashed_page__into_storage(page_content, backup_storage_full_path, hash_string)
            
            # Add hashed string to list
            file_names.append(os.path.join(backup_storage_full_path, hash_string[0], hash_string[1:]))

            # Calculate and log % done
            current_page += 1
            pages_done_percent = round(current_page * 100 / page_count)
            if (current_page >= page_current_step):
                await asyncio.sleep(0.2)
                logger.info(f"Creating incremental backup ({pages_done_percent:.0f}% done)")
                page_current_step += 0.1 * page_count
            elif (current_page == page_count):
                logger.info("Creating incremental backup (100% done)")
                
    # Write snapshot file
    async with aiofiles.open(backup_file_name_full_path, "w") as snapshot_file:
        for file_name in file_names:
            await snapshot_file.write(file_name + "\n")