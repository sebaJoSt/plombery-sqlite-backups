from datetime import datetime
from random import randint
import uvicorn
from plombery import Pipeline, _Plombery, get_logger, Trigger, task
from plombery_sqlite_backups import pipeline_inc_backup
from plombery_sqlite_backups.pipeline_full_backup import get_full_backup_pipeline

from apscheduler.triggers.interval import IntervalTrigger

def cli():
    app = _Plombery()
    app.register_pipeline(sales_pipeline)
    app.register_pipeline(get_full_backup_pipeline())
    uvicorn.run(app)
    #uvicorn.run("plombery:get_app", reload=True, factory=True)

@task
async def fetch_raw_sales_data():
    # using MarioPype logger your logs will be stored
    # and accessible on the web UI
    logger = get_logger()

    logger.debug("Fetching sales data...")

    sales = [
        {
            "price": randint(1, 1000),
            "store_id": randint(1, 10),
            "date": datetime.today(),
            "sku": randint(1, 50),
        }
        for _ in range(50)
    ]

    logger.info("Fetched %s sales data rows", len(sales))

    # Return the results of your task to have it stored
    # and accessible on the web UI
    return sales

sales_pipeline = Pipeline(
    id="sales_pipeline",
    tasks = [fetch_raw_sales_data],
    triggers = [
        Trigger(
            id="daily",
            name="Daily",
            description="Run the pipeline every day",
            schedule=IntervalTrigger(days=1),
        )
    ],
)




if __name__ == "__main__":
    cli()

