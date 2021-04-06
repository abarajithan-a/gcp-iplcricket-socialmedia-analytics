import json
import sys
from datetime import date, datetime, timedelta
from dateutil.tz import gettz
import requests
import math
from google.cloud import storage

bucket_name = "abar_ipl_twitter_feed"

bearer_token = "AAAAAAAAAAAAAAAAAAAAAAKvOAEAAAAAEUtdBuFpB%2FA5YFYfXkyGfAuSYCE%3D6kyefTwcw8HB0PCz2mgVouwjgGIqInXxoh8lOIB6IGaH7Dhq0F"

ipl_teams_twitter_handles = {
	"IPL" : "15639696",
	"CSK": "117407834",
  	"MI": "106345557",
  	"RCB": "70931004",
  	"SRH": "989137039",
  	"DC": "176888549",
  	"KKR": "23592970",
  	"RR": "17082958",
  	"KXIP": "30631766"
}

def pull_team_tweets(request):
	# Pulls the tweets from official IPL team handles for every 1 hour going back 3 days

	# Set date fields and parameters
	now = datetime.now(tz=gettz('Asia/Kolkata')).replace(tzinfo=None).replace(microsecond=0)
	now_utc = datetime.utcnow().replace(tzinfo=None).replace(microsecond=0)	

	#Round down to the nearest hour
	delta = timedelta(hours=1)
	now_down = datetime.min + math.floor((now - datetime.min) / delta) * delta
	offset = (now-now_down).total_seconds()
	now_utc = now_utc - timedelta(seconds=offset)
	now = now_down	

	# 2 hour tweet search window going back 3 days
	start_time = now - timedelta(days=3)
	start_time_utc = now_utc - timedelta(days=3)
	end_time_utc = start_time_utc + timedelta(hours=2)	

	# Set the date folder for the bucket
	folder = "ipl_teams_tweets/" + start_time.strftime("%Y-%m-%d")	

	#Convert to ISO format
	start_time = start_time.isoformat()
	now = now.isoformat()				
	start_time_param_utc = "start_time={}Z".format(start_time_utc.isoformat())
	end_time_param_utc = "end_time={}Z".format(end_time_utc.isoformat())

	tweet_fields = "tweet.fields=public_metrics,geo,created_at,entities,author_id,lang"
	max_results_field = "max_results=100"	

	for team_name, team_twitter_id in ipl_teams_twitter_handles.items():

		url = "https://api.twitter.com/2/users/{}/tweets?{}&{}&{}&{}".format(
		    team_twitter_id, max_results_field, tweet_fields, start_time_param_utc, end_time_param_utc
		)

		headers = {"Authorization": "Bearer {}".format(bearer_token)}	

		response = requests.request("GET", url, headers=headers)	

		if response.status_code != 200:
			err_message = team_name + ' tweet search failed for ' + start_time
			raise RuntimeError(err_message, response.text)
		else:		
			json_response = response.json()			

			# Serializing json 
			json_object = json.dumps(json_response, indent=4, sort_keys=True)		

			file_name = team_name + '_twitter_' + now + '.json'
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