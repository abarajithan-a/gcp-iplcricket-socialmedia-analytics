import json
import sys
from datetime import date, datetime, timedelta
from dateutil.tz import gettz
import requests
import math
from google.cloud import storage
from google.cloud import secretmanager

bucket_name = "abar_ipl_twitter_feed"
twitter_api_secret_name = 'abar-twitter-api-bearer-token'
gcp_project_id = 'abar-ipl-cricket'

ipl_teams_twitter_handles = {
	"IPL" : "15639696",
	"CSK": "117407834",
  	"MI": "106345557",
  	"RCB": "70931004",
  	"SRH": "989137039",
  	"DC": "176888549",
  	"KKR": "23592970",
  	"RR": "17082958",
  	"PBKS": "30631766",
  	"LSG": "4824087681",
  	"GT": "1476438846988427265"
}

def get_bearer_token_secret():
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret version.
    resource_name = f"projects/{gcp_project_id}/secrets/{twitter_api_secret_name}/versions/latest"
    # Access the secret version.
    response = client.access_secret_version(request={"name": resource_name})
    bearer_token = response.payload.data.decode("UTF-8")

    return bearer_token

def pull_team_tweets(request):
	# Pulls the tweets from official IPL team handles for every 1 hour going back 1 day

	# Get twitter api bearer token from GCP secret manager
	bearer_token = get_bearer_token_secret()

	# Set date fields and parameters
	now = datetime.now(tz=gettz('Asia/Kolkata')).replace(tzinfo=None).replace(microsecond=0)
	now_utc = datetime.utcnow().replace(tzinfo=None).replace(microsecond=0)	

	#Round down to the nearest hour
	delta = timedelta(hours=1)
	now_down = datetime.min + math.floor((now - datetime.min) / delta) * delta
	offset = (now-now_down).total_seconds()
	now_utc = now_utc - timedelta(seconds=offset)
	now = now_down	

	# 2 hour tweet search window going back 1 day
	start_time = now - timedelta(days=1)
	start_time_utc = now_utc - timedelta(days=1)
	end_time_utc = start_time_utc + timedelta(hours=2)	

	# Set the date folder for the bucket
	folder = "ipl_teams_tweets/" + start_time.strftime("%Y-%m-%d")	

	#Convert to ISO format
	start_time = start_time.isoformat()
	now = now.isoformat()				
	start_time_param_utc = "start_time={}Z".format(start_time_utc.isoformat())
	end_time_param_utc = "end_time={}Z".format(end_time_utc.isoformat())

	tweet_fields = "tweet.fields=attachments,public_metrics,geo,created_at,entities,author_id,lang,source"
	max_results_field = "max_results=100"
	exclude_field = "exclude=retweets"
	expansions_fields = "expansions=attachments.media_keys"
	media_fields = "media.fields=duration_ms,public_metrics"	

	for team_name, team_twitter_id in ipl_teams_twitter_handles.items():

		url = "https://api.twitter.com/2/users/{}/tweets?{}&{}&{}&{}&{}&{}&{}".format(
		    team_twitter_id, max_results_field, tweet_fields, start_time_param_utc, end_time_param_utc,
		    	exclude_field, expansions_fields, media_fields
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