import uuid
import os
from typing import Union
import requests
from bs4 import BeautifulSoup
import posthog


class AnonymousTracking(object):
    ANONYMOUS_USER_ID_FILE = '.user_id'
    FETCH_API_KEY_AND_URL = 'https://www.elementary-data.com/telemetry'

    def __init__(self, profiles_dir: str, anonymous_usage_tracking: bool = True) -> None:
        self.profiles_dir = profiles_dir
        self.anonymous_user_id = None
        self.api_key = None
        self.url = None
        self.do_not_track = anonymous_usage_tracking is False

    def init(self):
        try:
            anonymous_user_id_file_name = os.path.join(self.profiles_dir, self.ANONYMOUS_USER_ID_FILE)
            if os.path.exists(anonymous_user_id_file_name):
                with open(anonymous_user_id_file_name, 'r') as anonymous_user_id_file:
                    self.anonymous_user_id = anonymous_user_id_file.read()
            else:
                self.anonymous_user_id = str(uuid.uuid4())
                with open(anonymous_user_id_file_name, 'w') as anonymous_user_id_file:
                    anonymous_user_id_file.write(self.anonymous_user_id)

            self.api_key, self.url = self._fetch_api_key_and_url()
        except Exception:
            pass

    @classmethod
    def _fetch_api_key_and_url(cls) -> (Union[str, None], Union[str, None]):
        result = requests.get(url=cls.FETCH_API_KEY_AND_URL)
        if result.status_code != 200:
            return None, None
        soup = BeautifulSoup(result.content, 'html.parser')
        h5_tag = soup.find('h5')
        if h5_tag is None:
            return None, None

        api_key_and_url = h5_tag.text.split('\n')
        if len(api_key_and_url) != 2:
            return None, None

        return api_key_and_url[0], api_key_and_url[1]

    def send_event(self, name: str, properties: dict = None) -> None:
        if self.do_not_track:
            return

        if self.api_key is None or self.url is None or self.anonymous_user_id is None:
            return

        posthog.api_key = self.api_key
        posthog.host = self.url
        posthog.capture(distinct_id=self.anonymous_user_id, event=name, properties=properties)
