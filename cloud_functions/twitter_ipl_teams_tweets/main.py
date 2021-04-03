import json
import sys
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil.tz import gettz
import requests
import math
from google.cloud import storage

bucket_name = "abar_ipl_twitter_feed"

bearer_token = "AAAAAAAAAAAAAAAAAAAAAAKvOAEAAAAAEUtdBuFpB%2FA5YFYfXkyGfAuSYCE%3D6kyefTwcw8HB0PCz2mgVouwjgGIqInXxoh8lOIB6IGaH7Dhq0F"

ipl_teams_twitter_handles = {
	"CSK": "ChennaiIPL",
  	"MI": "mipaltan",
  	"RCB": "RCBTweets",
  	"SRH": "SunRisers",
  	"DC": "DelhiCapitals",
  	"KKR": "KKRiders",
  	"RR": "rajasthanroyals",
  	"KXIP": "PunjabKingsIPL"
}

def pull_team_tweets(request):
	# Pulls the tweets from official IPL team handles for every 1 hour going back 3 days
	# Set the date folder for the bucket
	folder = "ipl_teams_tweets/" + date.today().strftime("%Y-%m-%d")

	# Set date fields and parameters
	now=datetime.now(tz=gettz('Asia/Kolkata')).replace(tzinfo=None).replace(microsecond=0)	

	#Round down to the nearest hour
	delta = timedelta(hours=1)
	now = datetime.min + math.floor((now - datetime.min) / delta) * delta
	start_time = now - timedelta(days=3)
	start_time = datetime.min + math.floor((start_time - datetime.min) / delta) * delta
	end_time = start_time + delta	

	#Convert to ISO format
	start_time = start_time.isoformat()
	end_time = end_time.isoformat()		

	start_time_param = "start_time={}Z".format(start_time)
	end_time_param = "end_time={}Z".format(end_time)

	tweet_fields = "max_results=10&tweet.fields=public_metrics,geo,created_at,entities"	

	for team_name, team_handle in ipl_teams_twitter_handles.items():	

		query = "from:{} -is:retweet".format(team_handle)						

		url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
		    query, tweet_fields, start_time_param, end_time_param
		)

		headers = {"Authorization": "Bearer {}".format(bearer_token)}	

		response = requests.request("GET", url, headers=headers)	

		if response.status_code != 200:
			err_message = team_name + ' tweet search failed for ' + start_time
			raise RuntimeError(err_message, response.text)
		else:
			print(team_name + ' tweets search for ' + start_time + ' pulled successfully!')			
			json_response = response.json()			

			# Serializing json 
			json_object = json.dumps(json_response, indent=4, sort_keys=True)		

			file_name = team_name + '_twitter_' + now.isoformat() + '.json'
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

			print(team_name + ' tweets for ' + start_time + ' saved to GCP Cloud Storage successfully!')		