import sys
import json
import pandas as pd
import dask.dataframe as dd
import datetime
from datetime import timedelta
from itertools import combinations
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from google.cloud import storage

bucket_name = "abar_ipl_twitter_feed"
gcp_project_id = 'abar-ipl-cricket'
all_blobs = []

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

def list_all_blobs(tweets_date):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    all_blobs = client.list_blobs(bucket_name)
    tweet_blobs = []

    for blob in all_blobs:
        if (('ipl_teams_tweets' in blob.name or 'match_hashtag_tweets' in blob.name 
                or 'ipl_user_tweets' in blob.name) and 
                    (blob.time_created.strftime("%Y-%m-%d") == tweets_date)):
            tweet_blobs.append(blob)

    return tweet_blobs

def drop_df_columns(df):
    #Drop irrelevant columns
    drop_col_list = ['entities.annotations', 'entities.urls', 'geo.place_id', 'geo.coordinates']

    for c in drop_col_list:
        if c in df.columns:
            df.drop(columns=c, inplace=True)

def rename_df_columns(df):
    #rename columns
    rename_col_dict = {
        'id': 'tweet_id', 
        'source': 'tweet_source',
        'text': 'tweet_text',
        'entities.mentions': 'mentions',
        'public_metrics.like_count': 'like_count',
        'public_metrics.quote_count': 'quote_count',
        'public_metrics.reply_count': 'reply_count',
        'public_metrics.retweet_count': 'retweet_count',
        'entities.hashtags': 'hashtags'
    }

    df.rename(columns=rename_col_dict, inplace=True)

def add_df_sentiment_scores(df):
    vader = SentimentIntensityAnalyzer()
    df['sentiment_dict'] = df.apply(lambda x: vader.polarity_scores(x['tweet_text']),axis=1)
    df['positive_score'] = df.apply(lambda x: x['sentiment_dict']['pos'],axis=1)
    df['negative_score'] = df.apply(lambda x: x['sentiment_dict']['neg'],axis=1)
    df['neutral_score'] = df.apply(lambda x: x['sentiment_dict']['neu'],axis=1)
    df['compound_score'] = df.apply(lambda x: x['sentiment_dict']['compound'],axis=1)

    df.drop(columns='sentiment_dict', inplace=True)

def compute_author_name_type(author_id, file_name):
    if '_user_tweets' in file_name.lower():
        author_name = author_id
        author_type = 'user'
    elif '_match_hashtag_tweets_' in file_name.lower():
        if author_id in ipl_teams_twitter_handles.values():
            author_name = list(ipl_teams_twitter_handles.keys())[list(ipl_teams_twitter_handles.values()).index(author_id)]
            author_type = 'IPL team'
        else:
            author_name = author_id
            author_type = 'user'
    elif '_twitter_' in file_name.lower():
        author_name = list(ipl_teams_twitter_handles.keys())[list(ipl_teams_twitter_handles.values()).index(author_id)]
        author_type = 'IPL team'

    return (author_name, author_type)

def compute_ipl_game(hashtags, tweet_text, file_name):
    game = ''
    if '_match_hashtag_tweets_' in file_name.lower():
        game = file_name.lower().split("_")[0].replace('-', 'vs')
    else:
        # Check for Team1VsTeam2 text in hashtag or tweet_text
        teams = ipl_teams_twitter_handles.keys()
        matches_teams = list(combinations(teams, 2))

        for m in matches_teams:
            str1 = m[0].lower() + 'vs' + m[1].lower()
            str2 = m[1].lower() + 'vs' + m[0].lower()

            #check in hashtags
            if hashtags != '':
                for h in hashtags:
                    if str1 in h['tag'].lower():
                        game = str1
                        break
                    elif str2 in h['tag'].lower():
                        game = str2
                        break

            if game != '':
                break

            if tweet_text != '':
                if str1 in tweet_text.lower():
                    game = str1
                    break
                elif str2 in tweet_text.lower():
                    game = str2
                    break

    return game

def add_df_columns(df, ingestion_date):
    df['ingestion_date'] = ingestion_date
    df['ingestion_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df[['author_name','author_type']] = df.apply(lambda x: pd.Series(compute_author_name_type(x['author_id'], x['file_name'])), axis=1)
    df['game'] = df.apply(lambda x: compute_ipl_game(x['hashtags'],x['tweet_text'], x['file_name']), axis=1)

def generate_dask_raw_tweets_parquet(df, ingestion_date):
    col_list = ['author_id', 'created_at', 'tweet_id', 'lang', 'like_count', 'quote_count', 'reply_count', 
                    'retweet_count', 'tweet_source', 'tweet_text', 'positive_score', 'negative_score', 
                        'neutral_score', 'compound_score', 'file_name', 'ingestion_date', 
                            'ingestion_timestamp', 'author_name', 'author_type', 'game']
    partition_list = ['ingestion_date']
    ddf = dd.from_pandas(df[col_list], npartitions=10)
    ddf = ddf.persist()

    parquet_file_name = 'tweet_sentiment_scores'
    target_gcs_folder = 'twitter_parquets'

    parquet_folder_exists = False

    for blob in all_blobs:
        if 'tweet_sentiment_scores.parquet' in blob.name and (('ingestion_date=' + ingestion_date) in blob.name):
            parquet_folder_exists = True
            dd.to_parquet(ddf, 'gs://{}/{}/{}.parquet'.format(bucket_name, target_gcs_folder, parquet_file_name), 
                engine='pyarrow', write_index=False, append=True, partition_on=partition_list)
            break

    if not parquet_folder_exists:
        dd.to_parquet(ddf, 'gs://{}/{}/{}.parquet'.format(bucket_name, target_gcs_folder, parquet_file_name), 
            engine='pyarrow', write_index=False, append=False, partition_on=partition_list)

    print('Parquet - ' + parquet_file_name + ' for partition ' + ingestion_date + ' loaded into GCS successfully!')

def generate_dask_raw_hashtags_parquet(df, ingestion_date):
    col_list = ['author_id', 'created_at', 'tweet_id', 'hashtags', 'ingestion_timestamp', 
                    'file_name', 'ingestion_date']
    partition_list = ['ingestion_date']

    df1 = df[col_list].copy()
    df1 = df1.explode('hashtags').reset_index(drop=True)
    df1 = df1.join(pd.json_normalize(df1['hashtags']))
    df1.drop(columns=['hashtags', 'start', 'end'], inplace=True)
    df1.rename(columns={'tag': 'hashtag'}, inplace=True)

    ddf = dd.from_pandas(df1, npartitions=10)
    ddf = ddf.persist()

    parquet_file_name = 'tweet_hashtags'
    target_gcs_folder = 'twitter_parquets'

    parquet_folder_exists = False

    for blob in all_blobs:
        if 'tweet_hashtags.parquet' in blob.name and (('ingestion_date=' + ingestion_date) in blob.name):
            parquet_folder_exists = True
            dd.to_parquet(ddf, 'gs://{}/{}/{}.parquet'.format(bucket_name, target_gcs_folder, parquet_file_name), 
                engine='pyarrow', write_index=False, append=True, partition_on=partition_list)
            break

    if not parquet_folder_exists:
        dd.to_parquet(ddf, 'gs://{}/{}/{}.parquet'.format(bucket_name, target_gcs_folder, parquet_file_name), 
            engine='pyarrow', write_index=False, append=False, partition_on=partition_list)

    print('Parquet - ' + parquet_file_name + ' for partition ' + ingestion_date + ' loaded into GCS successfully!')
    del df1

def generate_dask_raw_mentions_parquet(df, ingestion_date):
    col_list = ['author_id', 'created_at', 'tweet_id', 'mentions', 'ingestion_timestamp', 
                    'file_name', 'ingestion_date']
    partition_list = ['ingestion_date']

    df1 = df[col_list].copy()
    df1 = df1.explode('mentions').reset_index(drop=True)
    df1 = df1.join(pd.json_normalize(df1['mentions']))
    df1.drop(columns=['mentions', 'start', 'end', 'id'], inplace=True)
    df1.rename(columns={'username': 'mention'}, inplace=True)

    ddf = dd.from_pandas(df1, npartitions=10)
    ddf = ddf.persist()

    parquet_file_name = 'tweet_mentions'
    target_gcs_folder = 'twitter_parquets'

    parquet_folder_exists = False

    for blob in all_blobs:
        if 'tweet_mentions.parquet' in blob.name and (('ingestion_date=' + ingestion_date) in blob.name):
            parquet_folder_exists = True
            dd.to_parquet(ddf, 'gs://{}/{}/{}.parquet'.format(bucket_name, target_gcs_folder, parquet_file_name), 
                engine='pyarrow', write_index=False, append=True, partition_on=partition_list)
            break

    if not parquet_folder_exists:
        dd.to_parquet(ddf, 'gs://{}/{}/{}.parquet'.format(bucket_name, target_gcs_folder, parquet_file_name), 
            engine='pyarrow', write_index=False, append=False, partition_on=partition_list)

    print('Parquet - ' + parquet_file_name + ' for partition ' + ingestion_date + ' loaded into GCS successfully!')
    del df1

def tweets_sentiment_parquet(df):
    drop_df_columns(df)
    rename_df_columns(df)
    add_df_sentiment_scores(df)
    
    #Replace Nan with empty string
    df.fillna('', inplace=True)

    ingestion_date = datetime.datetime.now().strftime("%Y-%m-%d")
    add_df_columns(df, ingestion_date)

    #write to tweets parquet files
    generate_dask_raw_tweets_parquet(df, ingestion_date)

    #write to tweet hashtags parquet files
    generate_dask_raw_hashtags_parquet(df, ingestion_date)

    #write to tweet mentions parquet files
    generate_dask_raw_mentions_parquet(df, ingestion_date)

def generate_parquet_files(request):
    # get the list of user tweet files to be processed
    tweets_date = (datetime.datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    tweet_blobs = list_all_blobs(tweets_date)

    df = pd.DataFrame()

    for blob in tweet_blobs:
        json_object = json.loads(blob.download_as_string())

        if 'data' in json_object.keys():
            temp_df = pd.json_normalize(json_object['data'], max_level=1)
            temp_df['file_name'] = blob.name.split('/')[2]
            df = df.append(temp_df)

    tweets_sentiment_parquet(df)

    print('Total Records {}', len(df.index))
    print(df.head())
