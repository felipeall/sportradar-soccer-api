from time import sleep

import requests
from requests.exceptions import ChunkedEncodingError

import sportradar_soccer_api.utils as utils


class SportradarSoccerAPI(object):

    session = requests.Session()

    def __init__(
        self,
        api_key,
        api="soccer-extended",
        level="trial",
        version="v4",
        language="en",
        api_format="json",
        timeout=120,
        sleep_time=1.1,
    ) -> None:

        self.api_key = f"?api_key={api_key}"
        self.api_root = f"http://api.sportradar.us/{api}/{level}/{version}/{language}/"
        self.api_format = f".{api_format}"
        self.timeout = timeout
        self.sleep_time = sleep_time

    def _make_request(self, api_endpoint):

        sleep(self.sleep_time)
        api_uri = self.api_root + api_endpoint + self.api_format + self.api_key

        try:
            response = self.session.get(api_uri, timeout=self.timeout)
            response.raise_for_status()

        except ChunkedEncodingError as e:
            print("Chunk Encoding Error:", e)
        except requests.exceptions.HTTPError as e:
            print("HTTP Error:", e)
        except requests.exceptions.ConnectionError as e:
            print("Error Connecting:", e)
        except requests.exceptions.Timeout as e:
            print("Timeout Error:", e)
        except requests.exceptions.RequestException as e:
            print("Error:", e)

        if response.status_code != 200:
            print(f"{api_endpoint} -> {response.status_code}")
            raise Exception("Invalid Status Code:", response.status_code)

        return response

    def get_competitions(self):
        response = self._make_request("competitions")
        competitions = utils.format_competitions(response)

        return competitions

    def get_seasons(self):
        response = self._make_request("seasons")
        seasons = utils.format_seasons(response)

        return seasons

    def get_season_summary(self, season_id: str):
        response = self._make_request(f"seasons/{season_id}/summaries")
        season_events_summaries = utils.format_season_summary(response)

        return season_events_summaries

    def get_season_players_statistics(self, season_id: str):
        response = self._make_request(f"seasons/{season_id}/summaries")
        season_players_statistics = utils.format_season_players_statistics(response)

        return season_players_statistics

    def get_season_competitors_statistics(self, season_id: str):
        response = self._make_request(f"seasons/{season_id}/summaries")
        season_competitors_statistics = utils.format_season_competitors_statistics(response)

        return season_competitors_statistics

    def get_season_referees(self, season_id: str):
        response = self._make_request(f"seasons/{season_id}/summaries")
        season_referees = utils.format_season_referees(response)

        return season_referees

    def get_season_ball_locations(self, season_id):
        response = self._make_request(f"seasons/{season_id}/summaries")
        season_ball_locations = utils.format_season_ball_locations(response)

        return season_ball_locations

    def get_season_channels(self, season_id):
        response = self._make_request(f"seasons/{season_id}/summaries")
        season_channels = utils.format_season_channels(response)

        return season_channels

    def get_player_profile(self, player_id: str):
        response = self._make_request(f"players/{player_id}/profile")
        player_profile = utils.format_player_profile(response)

        return player_profile
