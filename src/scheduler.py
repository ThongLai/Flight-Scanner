import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from daily_collector import DailyCollector

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_daily_collection():
    """Entry point for scheduled collection."""
    try:
        collector = DailyCollector()
        collector.run()
    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)


def start_scheduler():
    """Start the APScheduler with daily collection job."""
    scheduler = BlockingScheduler()

    scheduler.add_job(
        run_daily_collection,
        trigger=CronTrigger(hour=6, minute=0),  # 06:00 UTC daily
        id="daily_flight_collection",
        name="Daily flight price collection",
        misfire_grace_time=3600,  # run if missed by up to 1 hour
        coalesce=True,            # merge missed runs into one
    )

    logger.info("Scheduler started. Next run at 06:00 UTC daily.")
    logger.info(f"Jobs: {scheduler.get_jobs()}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
        scheduler.shutdown()


if __name__ == "__main__":
    start_scheduler()
