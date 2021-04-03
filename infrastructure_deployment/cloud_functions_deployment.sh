gcloud functions deploy twitter_ipl_teams_tweets \
	--source=../cloud_functions/twitter_ipl_teams_tweets \
	--entry-point=pull_team_tweets \
	--trigger-http \
	--region=us-east1 \
	--runtime=python37 \
	--memory=128MB \
	--timeout=540s \
	--allow-unauthenticated