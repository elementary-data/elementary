from elementary.config.config import Config
from elementary.tracking.anonymous_tracking import AnonymousTracking


class MockAnonymousTracking(AnonymousTracking):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(config=Config())
        self._do_not_track = True

    def _init(self):
        pass
