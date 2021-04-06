import json
import sys
from datetime import datetime, date, timedelta
from dateutil.tz import gettz
import requests
import urllib.parse
import math
import itertools
from google.cloud import storage

bucket_name = "abar_ipl_twitter_feed"

bearer_token = "AAAAAAAAAAAAAAAAAAAAAAKvOAEAAAAAEUtdBuFpB%2FA5YFYfXkyGfAuSYCE%3D6kyefTwcw8HB0PCz2mgVouwjgGIqInXxoh8lOIB6IGaH7Dhq0F"

ipl_teams_twitter_ids = {
	"IPL" : "15639696",
	"CSK": "117407834",
  	"MI": "106345557",
  	"RCB": "70931004",
  	"SRH": "989137039",
  	"DC": "176888549",
  	"KKR": "23592970",
  	"RR": "17082958",
  	"PBKS": "30631766"
}

# Dictionary to store all keywords related to IPL teams and the organization
dict_ipl_keywords = {
	'IPL': ['IPL', 'IPL ' + date.today().strftime("%Y"), 'Indian Premier League'],
	'CSK': ['csk', 'chennai super kings', 'yellove', 'whistlepodu'],
	'MI': ['mumbai indians', 'mipaltan', 'OneFamily'],
	'RCB': ['rcb', 'royal challengers bangalore', 'playbold', 'WeAreChallengers'],
	'SRH': ['srh', 'sunrisers hyderabad', 'OrangeArmy', 'OrangeOrNothing'],
	'DC': ['delhi capitals', 'YehHaiNayiDilli'],
	'KKR': ['kkr', 'Kolkata Knight Riders', 'KKRHaiTaiyaar', 'KorboLorboJeetbo'],
	'RR': ['Rajasthan Royals', 'HallaBol'],
	'PBKS': ['kxip', 'punjab kings', 'SaddaPunjab']
}

def get_twitter_data(url, start_time):
	headers = {"Authorization": "Bearer {}".format(bearer_token)}
	
	response = requests.request("GET", url, headers=headers)	

	if response.status_code != 200:
		err_message = 'user tweets search by failed for ' + start_time
		print(response.text)
		raise RuntimeError(err_message)
	else:	
		json_response = response.json()
		return json_response

def save_json_file(folder, file_name, start_time, json_response):
	# Serializing json 
	json_object = json.dumps(json_response, indent=4, sort_keys=True)		

	temp_file_path = '/tmp/{}'.format(file_name)		

	with open(temp_file_path, "w") as jsonfile:
		jsonfile.write(json_object)
			
	# set storage client
	client = storage.Client()
	# get bucket
	bucket = client.get_bucket(bucket_name)
	# set Blob
	blob = storage.Blob(folder + '/' + file_name, bucket)
	# upload the file to GCS
	blob.upload_from_filename(temp_file_path)

def user_tweets_by_matches(now, start_time, start_time_param, end_time_param):
	# Set the date folder for the bucket
	folder = "match_hashtag_tweets/" + start_time.strftime("%Y-%m-%d")

	# Set all the api parameters
	tweet_fields = "tweet.fields=public_metrics,geo,created_at,entities,author_id,lang"
	max_results_field = "max_results=100"
	expansions_fields = "expansions=geo.place_id"
	place_fields = "place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type"
	user_fields = "user.fields=location"

	list_ipl_teams = list({x for x in ipl_teams_twitter_ids if x not in 'IPL'})
	# Get all teh matchup permutations with length 2
	list_matchups = list(itertools.permutations(list_ipl_teams,2))
	dict_matches = {}

	# Build the matches list
	for teams in list_matchups:
		temp_list = list(teams)
		temp_list.sort()

		#sorted match key
		match_key = temp_list[0] + '-'+ temp_list[1]

		if match_key in dict_matches.keys():
			existing_list = dict_matches[match_key]
			existing_list.append(teams[0] + 'vs' + teams[1])
			dict_matches.update({match_key: existing_list})
		else:
			dict_matches[match_key] = [teams[0] + 'vs' + teams[1]]
	
	# Build the twitter api seach query 
	for key, matches in dict_matches.items():
		teams = key.split("-")
		team1_id = ipl_teams_twitter_ids[teams[0]]
		team2_id = ipl_teams_twitter_ids[teams[1]]

		search_keywords = '(' + matches[0] + ' OR ' + matches[1] + ')'
		not_team_ids = '(-from:' + team1_id + ' -from:' + team2_id + ')'
		no_retweets = '(-is:retweet)'

		query = '({} {} {})'.format(search_keywords, not_team_ids, no_retweets)
		# URL Encode the query string
		query_field = "query=" + urllib.parse.quote(query)

		url = "https://api.twitter.com/2/tweets/search/recent?{}&{}&{}&{}&{}&{}&{}&{}".format(
	    	query_field, max_results_field, tweet_fields, expansions_fields,
	    		place_fields, user_fields, start_time_param, end_time_param
		)
		
		json_response = get_twitter_data(url, start_time)
		file_name = key + '_match_hashtag_tweets_' + now + '.json'
		save_json_file(folder, file_name, start_time, json_response)	

def user_tweets_by_keywords(now, start_time, start_time_param, end_time_param):
	# Set the date folder for the bucket
	folder = "ipl_user_tweets/" + start_time.strftime("%Y-%m-%d")

	# Set all the api parameters
	tweet_fields = "tweet.fields=public_metrics,geo,created_at,entities,author_id,lang"
	max_results_field = "max_results=100"
	expansions_fields = "expansions=geo.place_id"
	place_fields = "place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type"
	user_fields = "user.fields=location"

	for team, keywords in dict_ipl_keywords.items():
		search_keywords = '(' + ' OR '.join(f'({k})' for k in keywords) + ')'
		not_team_id = '(-from:' + ipl_teams_twitter_ids[team] + ')'
		no_retweets = '(-is:retweet)'

		query = '({} {} {})'.format(search_keywords, not_team_id, no_retweets)
		# URL Encode the query string
		query_field = "query=" + urllib.parse.quote(query)

		url = "https://api.twitter.com/2/tweets/search/recent?{}&{}&{}&{}&{}&{}&{}&{}".format(
	    	query_field, max_results_field, tweet_fields, expansions_fields,
	    		place_fields, user_fields, start_time_param, end_time_param
		)

		json_response = get_twitter_data(url, start_time)
		file_name = team + '_user_tweets_' + now + '.json'
		save_json_file(folder, file_name, start_time, json_response)	

def search_user_tweets(request):
	# Set date fields and parameters
	now = datetime.now(tz=gettz('Asia/Kolkata')).replace(tzinfo=None).replace(microsecond=0)
	now_utc = datetime.utcnow().replace(tzinfo=None).replace(microsecond=0)	

	#Round down to the nearest 15 minute
	delta = timedelta(minutes=15)
	now_down = datetime.min + math.floor((now - datetime.min) / delta) * delta
	offset = (now-now_down).total_seconds()
	now_utc = now_utc - timedelta(seconds=offset)
	now = now_down	

	# 15 min tweet search window going back 3 days
	start_time = now - timedelta(days=3)
	start_time_utc = now_utc - timedelta(days=3)
	end_time_utc = start_time_utc + timedelta(minutes=15)	

	#Convert to ISO format
	now = now.isoformat()				
	start_time_param_utc = "start_time={}Z".format(start_time_utc.isoformat())
	end_time_param_utc = "end_time={}Z".format(end_time_utc.isoformat())

	# Pull tweets by keywords
	user_tweets_by_keywords(now, start_time, start_time_param_utc, end_time_param_utc)

	# Pull tweets by match hashtag Eg: CSKvsMI
	user_tweets_by_matches(now, start_time, start_time_param_utc, end_time_param_utc)		