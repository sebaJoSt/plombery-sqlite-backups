import uvicorn
import multiprocessing
from plombery_sqlite_backups import pipeline_full_backup, pipeline_inc_backup

def main():
    multiprocessing.freeze_support()
    uvicorn.run("plombery:get_app", reload=True, factory=True)

if __name__ == "__main__":
    main()