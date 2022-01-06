def GenerateConfig(context):
  # Create all the table definitions
  resources = [
    {
      'name': 'daily_ingest_twitter_tweets_raw',
      'type': 'bigquery.v2.table',
      'properties': {
        'datasetId': context.properties['datasetId'],
        'tableReference': {
          'tableId': 'daily_ingest_twitter_tweets_raw'
        },
        'type': 'TABLE',
        'schema': {
          'fields': [
            {'name': 'author_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'tweet_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'lang', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'like_count', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'quote_count', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'reply_count', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'retweet_count', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'tweet_source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'tweet_text', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'positive_score', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'negative_score', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'neutral_score', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'compound_score', 'type': 'STRING', 'mode': 'NULLABLE'},            
            {'name': 'ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'file_name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'author_name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'author_type', 'type': 'STRING', 'mode': 'REQUIRED'}                                                                                                                                                                      
          ]
        },
        'timePartitioning': {
          'type': 'DAY',
          'field': 'created_at'
        },
        'clustering': {
          'fields': ['author_type', 'tweet_source', 'lang']        
        }      
      }
    },
    {
      'name': 'daily_ingest_twitter_trends_raw',
      'type': 'bigquery.v2.table',
      'properties': {
        'datasetId': context.properties['datasetId'],
        'tableReference': {
          'tableId': 'daily_ingest_twitter_trends_raw'
        },
        'type': 'TABLE',
        'schema': {
          'fields': [
            {'name': 'location_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'location_woeid', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'as_of', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'trending_position', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'trending_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'promoted_content', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'tweet_volume', 'type': 'STRING', 'mode': 'NULLABLE'},             
            {'name': 'ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'file_name', 'type': 'STRING', 'mode': 'REQUIRED'}
          ]
        },
        'timePartitioning': {
          'type': 'DAY',
          'field': 'created_at'
        },
        'clustering': {
          'fields': ['trending_position']        
        }      
      }
    },
    {
      'name': 'daily_ingest_twitter_metadata_raw',
      'type': 'bigquery.v2.table',
      'properties': {
        'datasetId': context.properties['datasetId'],
        'tableReference': {
          'tableId': 'daily_ingest_twitter_metadata_raw'
        },
        'type': 'TABLE',
        'schema': {
          'fields': [
            {'name': 'as_of', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'entity_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'entity_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'entity_handle_username', 'type': 'STRING', 'mode': 'NULLABLE'},            
            {'name': 'entity_location', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'followers_count', 'type': 'STRING', 'mode': 'NULLABLE'}, 
            {'name': 'following_count', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'listed_count', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'tweet_count', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'file_name', 'type': 'STRING', 'mode': 'REQUIRED'}                                                                                                                                                                         
          ]
        },
        'timePartitioning': {
          'type': 'DAY',
          'field': 'as_of'
        },
        'clustering': {
          'fields': ['entity_location', 'entity_id']        
        }      
      }
    }
  ]
  return {'resources': resources}