#!/usr/bin/env python

import sys
import argparse 
import sqlite3
import twitter
import json
import time

def tweetexists(tweetid, cur):
    rows = cur.execute("SELECT id FROM tweets WHERE id = ?", (tweetid, )).fetchall()
    return (len(rows) > 0)

def resetsleeptime(api):
	try:
    	ratelimitstats = api.GetRateLimitStatus()
    	showstatuslimits = ratelimitstats['resources']['statuses']['/statuses/show/:id']
    	hitsremaining = showstatuslimits['remaining']
    	resettime = showstatuslimits['reset']
    	print("resettime = ", resettime, " time.time() = ", time.time() , " hit = ", hitsremaining)
    	sleeptime = int((int(resettime) - time.time()) / int(hitsremaining))
    	print("Sleeping", sleeptime, "seconds between API hits until", resettime)
    	return sleeptime, resettime, 1
	except:
		print("Unexpected error:", sys.exc_info()[0])
		return 0, 0, 0

def main(dbfile, query, consumerkey,
         consumersecret, accesstoken, accesstokensecret):

    api = twitter.Api(consumer_key=consumerkey,
                      consumer_secret=consumersecret,
                      access_token_key=accesstoken,
                      access_token_secret=accesstokensecret)

    conn = sqlite3.connect(dbfile, isolation_level=None)
    cur = conn.cursor()
    createtable(cur);

    [sleeptime, resettime, valid] = resetsleeptime(api)
    time.sleep(sleeptime)
    
    query_num = 1
    lastid = 0
    while 1:
        try:
            print (query)
            tweets = api.GetSearch(query, None, lastid, None, None, 100)
            for tweet in tweets:
                tweetid = tweet.id
                if tweetexists(tweetid, cur):
                    print ("Tweet with id:", tweetid, " is already expanded")
                    if lastid < tweetid:
                        lastid = tweetid   
                    continue
                print((str(int(time.time())) + " Expanding tweet with id:", tweetid))
                inserttweet(tweet, 0, cur)
                if lastid < tweetid:
                    lastid = tweetid
            print("Last id = ", lastid, " query = ", query_num)
            query_num = query_num + 1
        except twitter.TwitterError as err:
            print((str(int(time.time())) , " Error reading tweet id:", tweetid))
            print((str(err)))
        time.sleep(sleeptime) 
        if (time.time() >= resettime):
			valid = 0
			while not valid == 0:
            	[sleeptime, resettime, valid] = resetsleeptime(api);

    conn.close()

def boolToInt(bool):
    if (bool):
        return 1
    else:
        return 0

def createtable(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS tweets(
                created_at TEXT,
                eventindex INT,
                favorited INT,
                id INT PRIMARY KEY,
                text TEXT,
                location TEXT,
                in_reply_to_screen_name TEXT,
                in_reply_to_user_id TEXT,
                in_reply_to_status_id TEXT,
                truncated INT,
                source TEXT,
                urls TEXT,
                user_mentions TEXT,
                hashtags TEXT,
                geo TEXT,
                place TEXT,
                coordinates TEXT,
                contributors TEXT,
                retweeted INT,
                retweeted_status TEXT,
                retweet_count INT,
                user_id INT,
                user_name TEXT,
                user_screen_name TEXT,
                user_location TEXT,
                user_description TEXT,
                user_profile_image_url TEXT,
                user_profile_background_tile INT,
                user_profile_background_image_url TEXT,
                user_profile_sidebar_fill_color TEXT,
                user_profile_background_color TEXT,
                user_profile_link_color TEXT,
                user_profile_text_color TEXT,
                user_protected INT,
                user_utc_offset INT,
                user_time_zone TEXT,
                user_followers_count INT,
                user_friends_count INT,
                user_statuses_count INT,
                user_favourites_count INT,
                user_url TEXT,
                user_status TEXT,
                user_geo_enabled INT,
                user_verified INT,
                user_lang TEXT,
                user_notifications INT,
                user_contributors_enabled INT,
                user_created_at TEXT,
                user_listed_count INT
                )""")

def inserttweet_e(tweetid, eventindex, cur):
    cur.execute('INSERT INTO tweets VALUES('
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?) ',
                ("",
                eventindex,
                0,
                tweetid,
                "",
                "",
                "",
                "",
                "",
                0,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                0,
                "",
                0,
                -1,
                "",
                "",
                "",
                "",
                "",
                0,
                "",
                "",
                "",
                "",
                "",
                0,
                0,
                "",
                0,
                0,
                0,
                0,
                "",
                "",
                0,
                0,
                "",
                0,
                0,
                "",
                0,))

def inserttweet(tweet, eventindex, cur):
    cur.execute('INSERT INTO tweets VALUES('
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?) ',
                (tweet.created_at,
                eventindex,
                boolToInt(tweet.favorited),
                tweet.id,
                tweet.text,
                str(tweet.location),
                tweet.in_reply_to_screen_name,
                tweet.in_reply_to_user_id,
                tweet.in_reply_to_status_id,
                boolToInt(tweet.truncated),
                tweet.source,
                str(tweet.urls),
                str(tweet.user_mentions),
                str(tweet.hashtags),
                str(tweet.geo),
                str(tweet.place),
                str(tweet.coordinates),
                str(tweet.contributors),
                boolToInt(tweet.retweeted),
                str(tweet.retweeted_status),
                tweet.retweet_count,
                tweet.user.id,
                tweet.user.name,
                tweet.user.screen_name,
                tweet.user.location,
                tweet.user.description,
                tweet.user.profile_image_url,
                boolToInt(tweet.user.profile_background_tile),
                tweet.user.profile_background_image_url,
                tweet.user.profile_sidebar_fill_color,
                tweet.user.profile_background_color,
                tweet.user.profile_link_color,
                tweet.user.profile_text_color,
                boolToInt(tweet.user.protected),
                tweet.user.utc_offset,
                tweet.user.time_zone,
                tweet.user.followers_count,
                tweet.user.friends_count,
                tweet.user.statuses_count,
                tweet.user.favourites_count,
                tweet.user.url,
                tweet.user.status,
                boolToInt(tweet.user.geo_enabled),
                boolToInt(tweet.user.verified),
                tweet.user.lang,
                boolToInt(tweet.user.notifications),
                boolToInt(tweet.user.contributors_enabled),
                tweet.user.created_at,
                tweet.user.listed_count,))

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--dbfile', help='The sqlite3 database file to create and write to', required=True)
	parser.add_argument('--properties', help='The twitter properties file with necessary credentials', required=True)
	parser.add_argument('--query', help='The query used to search tweets', required=True) 
	args = parser.parse_args()

	#Create properties map
	properties = {}
	with open(args.properties, "r",-1, 'utf-8') as file_properties:
		for line in file_properties:
			terms = line.strip().split("=")
			properties[terms[0]]=terms[1]

	main(
		args.dbfile,
		args.query,
		properties["consumer_key"],
		properties["consumer_secret"],
		properties["access_token"],
		properties["access_token_secret"],
		)
