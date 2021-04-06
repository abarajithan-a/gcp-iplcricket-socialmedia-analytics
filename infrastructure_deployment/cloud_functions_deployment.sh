gcloud functions deploy twitter_ipl_teams_tweets \
	--source=../cloud_functions/twitter_ipl_teams_tweets \
	--entry-point=pull_team_tweets \
	--trigger-http \
	--region=us-east1 \
	--runtime=python37 \
	--memory=128MB \
	--timeout=540s \
	--allow-unauthenticated

gcloud functions deploy twitter_trends \
	--source=../cloud_functions/twitter_trends \
	--entry-point=pull_twitter_trends \
	--trigger-http \
	--region=us-east1 \
	--runtime=python37 \
	--memory=128MB \
	--timeout=540s \
	--allow-unauthenticated

gcloud functions deploy twitter_users_tweets \
	--source=../cloud_functions/twitter_users_tweets \
	--entry-point=search_user_tweets \
	--trigger-http \
	--region=us-east1 \
	--runtime=python37 \
	--memory=128MB \
	--timeout=540s \
	--allow-unauthenticated