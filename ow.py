import datetime
import elasticsearch
import requests
import time
import itertools


BASE_URI = "https://ow-api.com/v1/stats"
ES_ENDPOINT = "search-overwatch-a7ma4izaws4qpphgc5walk2lhy.us-west-2.es.amazonaws.com"
ES_PORT = 80
ES_INDEX = "overwatch"
ES_TYPE = "overwatch_stats"

class OverWatchPlayer(object):

    def __init__(self, battle_tag, platform=None, region=None):
        self.battle_tag = battle_tag
        self.platform = platform if platform else 'pc'
        self.region = region if region else 'us'

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return " -".join([self.battle_tag, self.platform, self.region])

class OverWatchDataProvider(object):

    def __init__(self, player, base_url=None):
        self.base_url = base_url if base_url else BASE_URI
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

    def parse_response(self, response, transform=False):
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
            row[u'@timestamp'] = timestamp
        else:
            timestamp = time.time()
            row[u'timestamp'] = timestamp
        return row

    @classmethod
    def execute_all(self):
        merge_candidate = [self.get_player_profile, self.get_player_complete, self.get_player_heroes]
        return list(itertools.chain.from_iterable(merge_candidate))

    @staticmethod
    def write_es_bulk(obj_list):
        from elasticsearch import Elasticsearch, helpers
        import hashlib
        #helpers = elasticsearch.helpers
        es = Elasticsearch([{'timeout': 30, 'host': ES_ENDPOINT, 'port': ES_PORT}])
        #previous_hour = datetime.now() - timedelta(hours=1)
        actions = [
                   {   
                    "_index": "overwatch", 
                    "_id" : hashlib.md5(
                                str(row.get("competitiveStats").get("games").get("played"))
                                + 
                                str(row.get("name"))
                                + 
                                str(row.get("quickPlayStats").get("games").get("played"))) \
                                .hexdigest(),
                    "_type": "overwatch_stats",
                    "_source": {
                        u"quickPlayStats": {
                            u"awards": {
                                u"cards": row.get("quickPlayStats").get("awards").get("cards"),
                                u"medals": row.get("quickPlayStats").get("awards").get("medals"),
                                u"medalsBronze": row.get("quickPlayStats").get("awards").get("medalsBronze"),
                                u"medalsGold": row.get("quickPlayStats").get("awards").get("medalsGold"),
                                u"medalsSilver": row.get("quickPlayStats").get("awards").get("medalsSilver")
                            },
                        u"games" : {
                            u"played":  row.get("quickPlayStats").get("games").get("played"),
                            u"won":  row.get("competitiveStats").get("games").get("won")
                        },
                        u"healingDoneAvg": row.get("quickPlayStats").get("healingDoneAvg"),
                        u"objectiveKillsAvg": row.get("quickPlayStats").get("objectiveKillsAvg"),
                        u"objectiveTimeAvg": row.get("quickPlayStats").get("objectiveTimeAvg"),
                        u"soloKillsAvg": row.get("quickPlayStats").get("soloKillsAvg"),
                        u"damageDoneAvg": row.get("quickPlayStats").get("damageDoneAvg"),
                        u"deathsAvg": row.get("quickPlayStats").get("deathsAvg"),
                        u"eliminationsAvg": row.get("quickPlayStats").get("eliminationsAvg"),
                        u"finalBlowsAvg": row.get("quickPlayStats").get("finalBlowsAvg"),
                        u"gamesWon": row.get("quickPlayStats").get("gamesWon"),   
                        u"timestamp": row.get("quickPlayStats").get("timestamp"),
                        },
                        u"competitiveStats": {
                            u"awards": {
                                u"cards": row.get("competitiveStats").get("awards").get("cards"),
                                u"medals": row.get("competitiveStats").get("awards").get("medals"),
                                u"medalsBronze": row.get("competitiveStats").get("awards").get("medalsBronze"),
                                u"medalsGold": row.get("competitiveStats").get("awards").get("medalsGold"),
                                u"medalsSilver": row.get("competitiveStats").get("awards").get("medalsSilver")
                            },
                            u"games" : {
                                u"played":  row.get("competitiveStats").get("games").get("played"),
                                u"won":  row.get("competitiveStats").get("games").get("won"),
                            },
                            u"healingDoneAvg": row.get("competitiveStats").get("healingDoneAvg"),
                            u"objectiveKillsAvg": row.get("competitiveStats").get("objectiveKillsAvg"),
                            u"objectiveTimeAvg": row.get("competitiveStats").get("objectiveTimeAvg"),
                            u"soloKillsAvg": row.get("competitiveStats").get("soloKillsAvg"),
                            u"damageDoneAvg": row.get("competitiveStats").get("damageDoneAvg"),
                            u"deathsAvg": row.get("competitiveStats").get("deathsAvg"),
                            u"eliminationsAvg": row.get("competitiveStats").get("eliminationsAvg"),
                            u"finalBlowsAvg": row.get("competitiveStats").get("finalBlowsAvg"),
                            u"gamesWon": row.get("competitiveStats").get("gamesWon"),   
                            
                        },
                    u"@timestamp": datetime.datetime.utcfromtimestamp(time.time()).isoformat(),
                    u"name": row.get("name"),
                    u"level": row.get("level"),
                    u"levelIcon": row.get("levelIcon"),
                    u"name": row.get("name"),
                    u"prestige": row.get("prestige"),
                    u"prestigeIcon": row.get("prestigeIcon"),
                    u"rating": row.get("rating"),
                    u"ratingIcon": row.get("ratingIcon"),
                    u"ratingName": row.get("ratingName"),
                    }
                }
            for row in obj_list

        ]
        helpers.bulk(es, actions)
        return True

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
        "battle_tag": "Problem#1389",
        },
        {
        "battle_tag": "CAIRO#11674"
        },
        {
        "battle_tag": "Ropenhagen#1408"
        },
        {
        "battle_tag": "Atothendrew#1548"
        },
        {
        "battle_tag": "JustEpiC#11564"
        },
        {
        "battle_tag": "Crazyeye#1402"
        },
        {
        "battle_tag": "WoodEngy#1597"
        },
        {
        "battle_tag": "Nothingg#11382"
        },
        {
        "battle_tag": "Solution#11404"
        }

    ]
}

if __name__ == '__main__':
    # create player object - defaults to us, pc
    #player = OverWatchPlayer(battle_tag="Problem#1389")
    players = (OverWatchPlayer(row.get("battle_tag"), row.get("platform"), row.get("region")) for row in group_dict.get("players"))
    # pull player stats - basic profile stats - comp, quickplay
    #profile = OverWatchDataProvider(player).get_player_profile()
    profiles = (OverWatchDataProvider(player).get_player_profile() for player in players)
    #write the data to ES
    #OverWatchDataProvider.write_es_bulk([profile])
    OverWatchDataProvider.write_es_bulk(list(profiles))
    #complete = OverWatchDataProvider(player).get_player_complete()
    #heroes = OverWatchDataProvider(player).get_player_heroes(['mercy'])
    #print(list(profiles))
    #team = OverWatchGroupBuilder(group_dict).build_team_info()
    #team_stats = OverWatchGroupProcess(team).get_team_data()
