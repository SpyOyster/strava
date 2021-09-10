import sys
import json
import gzip
import logging
import requests
import os
from google.cloud import bigquery
from google.cloud import storage

CLIENTID = '*****'
CLIENTSECRET = '******'
GCSSTORAGE = 'bucket-name'

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def fetch_strava_access_token():

    logging.info('Fetching Strava Refresh Token from GCS...')
    client = storage.Client()
    bucket = client.get_bucket(GCSSTORAGE)
    strava_refresh_token_file = 'strava_refresh_token.txt'
    token_file = bucket.get_blob(strava_refresh_token_file)
    REFRESHTOKEN = token_file.download_as_string()
    
    logging.info('Fetching Strava Access Token ...')
    resp = requests.post(
            'https://www.strava.com/api/v3/oauth/token',
            params={f'client_id': {CLIENTID}, 'client_secret': {CLIENTSECRET}, 'grant_type': 'refresh_token', 'refresh_token': {REFRESHTOKEN}, 'scope': 'activity:read_all'}
        )
    response = resp.json()
    
    logging.info('Pushing Strava Refresh Token to GCS...')   
    token_file.upload_from_string(response['refresh_token'])
    return response['access_token']

def fetch_strava_activities(STRAVA_ACCESS_TOKEN):
    page, activities = 1, []

    logging.info('Fetching Strava Activities ...')
    while True:
        logging.info(f'Fetching page #{page} ...')
        resp = requests.get(
            'https://www.strava.com/api/v3/athlete/activities',
            headers={'Authorization': f'Bearer {STRAVA_ACCESS_TOKEN}'},
            params={'page': page, 'per_page': 200}
        )
        data = resp.json()
        activities += data
        if len(data) < 200:
            break
        page += 1 
        
    logging.info(f'Fetched {len(activities)} activites')
    logging.info(f'activities: {activities}')
    return activities
    
def activites_to_bq(jsonl_lines, project, dataset, table):
    bq_client = bigquery.Client()
    job_config = bigquery.job.LoadJobConfig()

    job_config.source_format = bigquery.job.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE # Overwrite
    job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
    job_config.autodetect = True

    logging.info(f'Starting import to {project}.{dataset}.{table} ...')
    
    job = bq_client.load_table_from_json(
        json_rows=jsonl_lines,
        destination=f'{project}.{dataset}.{table}',
        job_config=job_config
    )

    logging.info(f'Launched job id: {job.job_id}')
    return job.job_id

def run(data, context=None):
    STRAVA_ACCESS_TOKEN = fetch_strava_access_token()
    logging.info(f'STRAVA_ACCESS_TOKEN: #{STRAVA_ACCESS_TOKEN}')
    activities = fetch_strava_activities(STRAVA_ACCESS_TOKEN)
    activites_to_bq(activities,'datalake-team-poc', 'az02194', 'strava_activities')
