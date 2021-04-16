import json
import sys
from datetime import date, datetime, timedelta
from dateutil.tz import gettz
import requests
from google.cloud import storage
from google.cloud import secretmanager

bucket_name = "abar_ipl_twitter_feed"
twitter_api_secret_name = 'abar-twitter-api-bearer-token'
gcp_project_id = 'abar-ipl-cricket'

def get_bearer_token_secret():
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret version.
    resource_name = f"projects/{gcp_project_id}/secrets/{twitter_api_secret_name}/versions/latest"
    # Access the secret version.
    response = client.access_secret_version(request={"name": resource_name})
    bearer_token = response.payload.data.decode("UTF-8")

    return bearer_token

def pull_twitter_trends(request):
	# Get twitter api bearer token from GCP secret manager
	bearer_token = get_bearer_token_secret()
	
	# Pulls the current worldwide top 50 trending hashtags
	# Set date fields and parameters
	now=datetime.now(tz=gettz('Asia/Kolkata')).replace(tzinfo=None).replace(microsecond=0)

	# Set the date folder for the bucket
	folder = "twitter_trends/" + now.strftime("%Y-%m-%d")

	now=now.isoformat()					

	# Worldwide trends with id = 1
	url = "https://api.twitter.com/1.1/trends/place.json?id=1"

	headers = {"Authorization": "Bearer {}".format(bearer_token)}	

	response = requests.request("GET", url, headers=headers)	

	if response.status_code != 200:
		err_message = 'Worldwide twitter trends pull failed for ' + now
		raise RuntimeError(err_message, response.text)
	else:		
		json_response = response.json()			

		# Serializing json 
		json_object = json.dumps(json_response, indent=4, sort_keys=True)		

		file_name = 'twitter_trends_' + now + '.json'
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

		print('Worldwide twitter trends at ' + now + ' saved to GCP Cloud Storage successfully!')		