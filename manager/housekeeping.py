import time

from app import logger, start_background_services


def main():
    start_background_services()
    logger.info("Thinvids manager housekeeping started")
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
