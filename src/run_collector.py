import argparse
import logging
import sys

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
    )
    parser.add_argument(
        "--force",
        action="store_true",
    )
    args = parser.parse_args()

    if args.mode == "once":
        from daily_collector import DailyCollector
        collector = DailyCollector()
        healthy = collector.run(force=args.force)
        if healthy is False:
            sys.exit(1)  # this can trigger GitHub Actions failure email
    else:
        from scheduler import start_scheduler
        start_scheduler()
        from scheduler import start_scheduler
        start_scheduler()


if __name__ == "__main__":
    main()
