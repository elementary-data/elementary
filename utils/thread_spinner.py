import alive_progress
import time
from threading import Thread


class ThreadSpinner(object):
    def __init__(self, title: str) -> None:
        self.stop_spinner = False
        self.thread = Thread(target=self._run_spinner, args=(title,))

    def _run_spinner(self, title: str) -> None:
        with alive_progress.alive_bar(title=title, bar=None, spinner='twirl', monitor=False) as spinner:
            while not self.stop_spinner:
                time.sleep(0.2)
                spinner()

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_spinner = True
        self.thread.join()
        self.stop_spinner = False

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, traceback):
        self.stop()
