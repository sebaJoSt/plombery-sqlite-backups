import uvicorn
from plombery_sqlite_backups import pipeline_full_backup, pipeline_inc_backup

def main():
    uvicorn.run("plombery:get_app", reload=True, factory=True)

if __name__ == "__main__":
    main()