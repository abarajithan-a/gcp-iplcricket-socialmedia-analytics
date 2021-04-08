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

def pull_user_metadata(request):
	# Pulls the metadata for official IPL team handles for every day
	# Set date fields and parameters
	now=datetime.now(tz=gettz('Asia/Kolkata')).replace(tzinfo=None).replace(microsecond=0)

	# Set the date folder for the bucket
	folder = "twitter_ipl_metadata/" + now.strftime("%Y-%m-%d")

	now=now.isoformat()		

	user_fields = "user.fields=entities,location,public_metrics"

	for team_name, team_twitter_id in ipl_teams_twitter_handles.items():

		url = "https://api.twitter.com/2/users/{}?{}".format(
		    team_twitter_id, user_fields
		)

		headers = {"Authorization": "Bearer {}".format(bearer_token)}	

		response = requests.request("GET", url, headers=headers)	

		if response.status_code != 200:
			err_message = team_name + ' user metadata search failed for ' + now
			raise RuntimeError(err_message, response.text)
		else:		
			json_response = response.json()			

			# Serializing json 
			json_object = json.dumps(json_response, indent=4, sort_keys=True)		

			file_name = team_name + '_twitter_metadata_' + now + '.json'
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