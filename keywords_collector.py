"""
Google Business Profile - Search Keywords Collector
Cloud Function to fetch monthly search keywords and store in BigQuery
"""

import requests
import logging
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.auth import default
from google.auth.transport.requests import Request

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = os.environ.get('BQ_DATASET', 'google_business_profile')
TABLE_ID = os.environ.get('BQ_TABLE_KEYWORDS', 'search_keywords')
MONTHS_BACK = int(os.environ.get('MONTHS_BACK', '3'))  # Default 3 months


def get_credentials():
    """Get Application Default Credentials with proper scopes"""
    credentials, project = default(scopes=[
        'https://www.googleapis.com/auth/business.manage'
    ])
    
    if not credentials.valid:
        credentials.refresh(Request())
    
    return credentials


def get_all_locations(credentials):
    """Get all locations using accounts/- wildcard"""
    headers = {
        'Authorization': f'Bearer {credentials.token}',
        'Content-Type': 'application/json'
    }
    
    read_mask = 'name,title,storefrontAddress,metadata'
    url = 'https://mybusinessbusinessinformation.googleapis.com/v1/accounts/-/locations'
    params = {'readMask': read_mask, 'pageSize': 100}
    
    all_locations = []
    
    while True:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'locations' in data:
                all_locations.extend(data['locations'])
                logger.info(f"Fetched {len(data['locations'])} locations")
            
            if 'nextPageToken' in data:
                params['pageToken'] = data['nextPageToken']
            else:
                break
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching locations: {e}")
            break
    
    return all_locations


def get_search_keywords(credentials, location_name, months_back=3):
    """Get search keywords for a location"""
    headers = {
        'Authorization': f'Bearer {credentials.token}',
        'Content-Type': 'application/json'
    }
    
    location_id = location_name.split('/')[-1]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months_back*30)
    
    url = f'https://businessprofileperformance.googleapis.com/v1/locations/{location_id}/searchkeywords/impressions/monthly'
    
    params = {
        'monthlyRange.start_month.year': start_date.year,
        'monthlyRange.start_month.month': start_date.month,
        'monthlyRange.end_month.year': end_date.year,
        'monthlyRange.end_month.month': end_date.month,
        'pageSize': 100
    }
    
    all_keywords = []
    
    while True:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'searchKeywordsCounts' in data:
                all_keywords.extend(data['searchKeywordsCounts'])
                logger.info(f"Fetched {len(data['searchKeywordsCounts'])} keywords")
            
            if 'nextPageToken' in data:
                params['pageToken'] = data['nextPageToken']
            else:
                break
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching keywords for {location_name}: {e}")
            break
    
    return all_keywords


def extract_keyword_value(search_keyword):
    """Extract keyword string from nested structure"""
    if isinstance(search_keyword, dict):
        return search_keyword.get('string', 'UNKNOWN')
    return str(search_keyword)


def extract_impressions(insights_value):
    """Extract impression count from nested structure"""
    if isinstance(insights_value, dict) and 'value' in insights_value:
        try:
            return int(insights_value.get('value', 0))
        except (ValueError, TypeError):
            return 0
    return 0


def transform_to_bigquery_rows(all_keywords_data):
    """Transform raw keyword data to BigQuery-ready rows"""
    rows = []
    current_timestamp = datetime.utcnow().isoformat()
    
    for record in all_keywords_data:
        keyword = extract_keyword_value(record.get('searchKeyword', {}))
        impressions = extract_impressions(record.get('insightsValue', {}))
        
        # Skip keywords with zero impressions
        if impressions == 0 or keyword == 'UNKNOWN':
            continue
        
        row = {
            'collection_date': datetime.utcnow().date().isoformat(),
            'location_name': record['location_name'],
            'location_title': record['location_title'],
            'keyword': keyword,
            'impressions': impressions,
            'months_period': MONTHS_BACK,
            'data_fetched_at': current_timestamp
        }
        
        rows.append(row)
    
    return rows


def write_to_bigquery(rows):
    """Write rows to BigQuery with deduplication"""
    if not rows:
        logger.warning("No rows to write to BigQuery")
        return
    
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    try:
        # Delete existing records for today's collection
        collection_date = datetime.utcnow().date().isoformat()
        
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE collection_date = '{collection_date}'
        """
        
        logger.info(f"Deleting existing records for {collection_date}")
        client.query(delete_query).result()
        
        # Insert new records
        logger.info(f"Inserting {len(rows)} new keyword records")
        errors = client.insert_rows_json(table_ref, rows)
        
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
            raise Exception(f"Failed to insert rows: {errors}")
        
        logger.info(f"Successfully wrote {len(rows)} rows to BigQuery")
        
    except Exception as e:
        logger.error(f"BigQuery write error: {e}")
        raise


def main(request=None):
    """
    Main function for Cloud Function
    Can be triggered by HTTP request or Cloud Scheduler
    """
    logger.info("="*70)
    logger.info("Starting Search Keywords Collection")
    logger.info("="*70)
    
    logger.info(f"Collection period: Last {MONTHS_BACK} months")
    logger.info(f"Target table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    
    try:
        # Get credentials
        logger.info("Authenticating...")
        credentials = get_credentials()
        
        # Get all locations
        logger.info("Fetching locations...")
        locations = get_all_locations(credentials)
        logger.info(f"Found {len(locations)} location(s)")
        
        if len(locations) == 0:
            logger.error("No locations found")
            return {"status": "error", "message": "No locations found"}, 400
        
        # Collect keywords for all locations
        all_keywords_data = []
        locations_with_data = 0
        
        for idx, location in enumerate(locations, 1):
            location_name = location['name']
            location_title = location.get('title', 'N/A')
            
            logger.info(f"[{idx}/{len(locations)}] Processing: {location_title}")
            
            # Get keywords
            keywords = get_search_keywords(credentials, location_name, months_back=MONTHS_BACK)
            
            if keywords:
                logger.info(f"  ✅ Got {len(keywords)} keywords")
                locations_with_data += 1
                
                # Add location info to each keyword record
                for keyword_record in keywords:
                    keyword_record['location_name'] = location_name
                    keyword_record['location_title'] = location_title
                    
                all_keywords_data.extend(keywords)
            else:
                logger.warning(f"  ⚠️ No keyword data for {location_title}")
        
        # Transform and write to BigQuery
        logger.info(f"Transforming {len(all_keywords_data)} keyword records...")
        rows = transform_to_bigquery_rows(all_keywords_data)
        
        logger.info(f"Valid keywords to write: {len(rows)}")
        
        if rows:
            logger.info("Writing to BigQuery...")
            write_to_bigquery(rows)
        else:
            logger.warning("No valid keywords to write - locations may not have enough traffic")
        
        logger.info("="*70)
        logger.info("✅ Keyword collection complete!")
        logger.info(f"Locations processed: {len(locations)}")
        logger.info(f"Locations with data: {locations_with_data}")
        logger.info(f"Keywords written: {len(rows)}")
        logger.info("="*70)
        
        return {
            "status": "success",
            "keywords_written": len(rows),
            "locations_processed": len(locations),
            "locations_with_data": locations_with_data,
            "months_period": MONTHS_BACK
        }, 200
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}, 500


# For local testing
if __name__ == "__main__":
    result, status_code = main()
    print(f"\nResult ({status_code}): {result}")
