import datetime
import requests
import time
import itertools

class OverWatchPlayer(object):

    def __init__(self, battle_tag, platform, region):
        self.battle_tag = battle_tag
        self.platform = platform
        self.region = region

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return " -".join([self.battle_tag, self.platform, self.region])

class OverWatchDataProvider(object):

    def __init__(self, player, base_url=None):
        self.base_url = base_url if base_url else "https://ow-api.com/v1/stats"
        self.player = player
        self.region = player.region
        self.battle_tag = player.battle_tag
        self.platform = player.platform

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def build_url(self, battle_tag, endpoint, heroes=None):
        if heroes:
            url = '/'.join([self.base_url, self.platform, self.region, self.battle_tag.replace('#', '-'), endpoint])
            params = ','.join(heroes)
            return '/'.join([url, params])
        else:
            return '/'.join([self.base_url, self.platform, self.region, self.battle_tag.replace('#', '-'), endpoint])

    def dispatch_request(self, url):
        import requests
        # Make a request
        print("Requesting data for " + url)

        try :
            response = requests.get(url)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(e)
        print("Received good response from API endpoint " + str(response.status_code))
        return self.parse_response(response)

    def parse_response(self, response, transform=True):
        data = response.json()       
        if "error" in data.keys():
            print("Request failed through API proxy")
        else:
            if transform:
                return self.transform_data(data)
            else:
                return data

    def get_player_profile(self, transform=True):
        return self.dispatch_request(self.build_url(self.player.battle_tag, endpoint="profile"))

    def get_player_complete(self, transform=True):
        return self.dispatch_request(self.build_url(self.player.battle_tag, endpoint="complete"))

    def get_player_heroes(self, hero_list, transform=True):
        return self.dispatch_request(self.build_url(self.player.battle_tag, endpoint="heroes", heroes=hero_list))

    @staticmethod
    def transform_data(row, convert_time=True):
        if convert_time:
            timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')
            row[u'timestamp'] = timestamp
        else:
            timestamp = time.time()
            row[u'timestamp'] = timestamp
        return row

    @classmethod
    def execute_all(self):
        merge_candidate = [self.get_player_profile, self.get_player_complete, self.get_player_heroes]
        return list(itertools.chain.from_iterable(merge_candidate))

class OverWatchGroupBuilder(object):
    def __init__(self, group_dict):
        self.team = group_dict.get('players')

    def build_team_info(self):
        return self.team

class OverWatchGroupProcess(object):
    def __init__(self, team):
        self.team = team
    def get_player_objects(self):
        players = OverWatchPlayer()
        return [OverWatchPlayer(player['battle_tag'], player['platform'], player['region']) in self.team]
    def get_team_data(self):
        return [OverWatchDataProvider(player) for player in self.get_player_objects()]

group_dict = {
    "players": [
        {
        "battle_tag": "ghavek#1129",
        "platform": "pc",
        "region": "us"
        },
        {
        "battle_tag": "cairo",
        "platform": "pc",
        "region": "us"
        }
    ]
}

player = OverWatchPlayer(battle_tag="Lenorellei#1694", platform="pc", region="us")
profile = OverWatchDataProvider(player).get_player_profile()
complete = OverWatchDataProvider(player).get_player_complete()
heroes = OverWatchDataProvider(player).get_player_heroes(['mercy'])
team = OverWatchGroupBuilder(group_dict).build_team_info()
team_stats = OverWatchGroupProcess(team).get_team_data()
