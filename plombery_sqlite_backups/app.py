from plombery_sqlite_backups import pipeline_full_backup, pipeline_inc_backup

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("plombery:get_app", reload=True, factory=True)
