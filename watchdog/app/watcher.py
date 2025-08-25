import requests
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import PatternMatchingEventHandler
import time
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VideoFileHandler(PatternMatchingEventHandler):
    def __init__(self):
        super().__init__(patterns=['*.mkv', '*.mp4'], ignore_directories=True, case_sensitive=False)

    def on_created(self, event):
        path = event.src_path
        filename = os.path.basename(path)
        logger.info(f"File created: {path}, waiting for stabilization...")

        if self.wait_until_stable(path):
            logger.info(f"[INFO] File is stable: {filename}, submitting job.")
            try:
                response = requests.post("http://flaskapp:5000/add_job", json={"filename": path.replace("/watch/", "")})
                logger.info(f"Job submitted: {response.status_code}")
            except Exception as e:
                logger.error(f"Failed to submit job: {e}")
        else:
            logger.warning(f"File never stabilized: {filename}, skipping.")

    def wait_until_stable(self, path, checks=5, delay=10):
        last_size = -1
        stable_count = 0
        while True:
            try:
                current_size = os.path.getsize(path)
            except FileNotFoundError:
                return False
            if current_size == last_size:
                stable_count += 1
                if stable_count >= checks:
                    return True
            else:
                stable_count = 0
            last_size = current_size
            time.sleep(delay)
        return False

def start_watcher():
    path = '/watch'
    logger.info(f" Watching directory: {path}")
    event_handler = VideoFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == '__main__':
    start_watcher()
