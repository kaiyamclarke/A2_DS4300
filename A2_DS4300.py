import json
import pandas as pd
from datetime import datetime as dt, timedelta as td
import redis
import numpy as np
import pdb


def postTweets(row: pd.Series, engine):
    """
    :param row: serves as the Tweet object, corresponds to a row of the csv file
    :param engine: the Redis connection to our database
    :return: None; Posts a tweet to the database at the current time
    """
    tweet = {
        'user_id': int(row['USER_ID']),
        'tweet_ts': dt.now().strftime("%Y-%m-%d %H:%M:%S"),
        'tweet_text': row['TWEET_TEXT']
    }

    engine.publish('tweets', json.dumps(tweet))
    updateTimelines(engine)


def updateTimelines(engine):
    """
    :param engine: the Redis connection to our database
    :return: None;
    """
    sub = engine.pubsub()
    sub.subscribe('tweets')
    for message in sub.listen():
        if message['type'] == 'message':
            tweet = json.loads(message['data'])
            followers = engine.smembers(f'followers:{tweet["user_id"]}')
            for follower in followers:
                timeline_key = f'timeline:{follower}'
                tweet_data = f'{tweet["user_id"]} - {tweet["tweet_text"]}'
                engine.lpush(timeline_key, tweet_data)


def postFollows(row: pd.Series, engine):
    """
    :param row: serves as the Follows-Relationship object, corresponds to a row of the csv file
    :param engine: the Redis connection to our database
    :return: None; Posts a follow relationship to the database
    """
    user_id = int(row['USER_ID'])
    follows_id = int(row['FOLLOWS_ID'])

    engine.sadd(f'followers:{user_id}', follows_id)


def getHomeTimeline(engine, user: int):
    """
    :param user: the user_id of the user whose timeline we are viewing
    :param engine: the Redis connection to our database
    :return: A dataframe showing the top 10 Tweets on a random user's timeline
    """
    timeline_key = f'timeline:{user}'
    tweets = engine.lrange(timeline_key, 0, 9)

    print(f'timeline for user {user}:')
    print(tweets)


# def getAllTimelineKeys(engine):
#     """
#     :param engine: the Redis connection to our database
#     :return: A list of all existing timeline keys
#     """
#     cursor = 0
#     timeline_keys = []
#
#     while True:
#         cursor, keys = engine.scan(cursor, match='timeline:*')
#         timeline_keys.extend(keys)
#
#         # Break the loop when the cursor is 0, indicating the end of the keyspace
#         if cursor == 0:
#             break
#
#     return list(map(lambda key: key.split(":")[1], timeline_keys))


# Driver program without specific knowledge of underlying database
def main():

    # Establish valid database connection
    r = redis.Redis(host='localhost', port=6379)

    # Clear the database
    r.flushall()

    # Read in follows.csv and tweet.csv data to Pandas DataFrames
    follows = pd.read_csv('data/follows.csv')
    tweets: pd.DataFrame = pd.read_csv('data/tweet.csv')

    # POSTING FOLLOWS ----------------------------------
    print('Starting follow uploads...')
    start_time = dt.now()
    for i in range(len(follows)):  # For each follow relationship,
        postFollows(row=follows.iloc[i], engine=r)  # Post it to the database
    end_time = dt.now()
    print(f'Done! This took {end_time - start_time} time.')
    # --------------------------------------------------

    # POSTING TWEETS ----------------------------------
    print('Starting tweet uploads...')
    start_time = dt.now()
    # for i in range(len(tweets)):  # For each Tweet,
    for i in range(10):
        print(i)
        postTweets(row=tweets.iloc[i], engine=r)  # Post it to the database
    end_time = dt.now()
    print(f'Done! This took {end_time - start_time} time.')
    # --------------------------------------------------

    # GETTING TIMELINES ----------------------------------
    print('Getting timelines...')
    tls = 0
    timeline_duration = 0.25  # Number of minutes to run getHomeTimeline for
    timeline_end = dt.now() + td(minutes=timeline_duration)
    tl = {}
    while dt.now() < timeline_end:  # For the specified time,
        tl = getHomeTimeline(engine=r, user=np.random.choice(follows['USER_ID'].values))  # Get a random user's home timeline
        tls += 1
    print('Last timeline:\n', tl, '\n')
    print(f'We got {tls} timelines in {timeline_duration} minutes,'
          f'\nor {tls/(timeline_duration * 60)} per second.')
    # --------------------------------------------------


main()
