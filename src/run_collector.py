import argparse
import logging

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)



def main():
    parser = argparse.ArgumentParser(description="Flight price collector")
    parser.add_argument(
        "--mode",
        choices=["once", "scheduled"],
        default="scheduled",
        help="Run once or start scheduler",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force collection even if already collected today",
    )
    args = parser.parse_args()

    if args.mode == "once":
        from daily_collector import DailyCollector
        collector = DailyCollector()
        collector.run(force=args.force)
    else:
        from scheduler import start_scheduler
        start_scheduler()


if __name__ == "__main__":
    main()
