from crusher import twitter as tw
from live_feed.twitter_listener import listen_for_teams


def run(teams):
    listen_for_teams(teams)


if __name__ == 'main':
    teams_list = []
    run(teams_list)
