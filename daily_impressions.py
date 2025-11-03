"""
Google Business Profile - Daily Impressions Data Collection
Cloud Function to fetch last 90 days of daily metrics and store in BigQuery
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
TABLE_ID = os.environ.get('BQ_TABLE_IMPRESSIONS', 'daily_impressions')


def get_credentials():
    """Get Application Default Credentials with proper scopes"""
    credentials, project = default(scopes=[
        'https://www.googleapis.com/auth/business.manage'
    ])
    
    # Refresh token if needed
    if not credentials.valid:
        credentials.refresh(Request())
    
    return credentials


def get_all_locations(credentials):
    """Get all locations using accounts/- wildcard"""
    headers = {
        'Authorization': f'Bearer {credentials.token}',
        'Content-Type': 'application/json'
    }
    
    read_mask = 'name,title,storefrontAddress,phoneNumbers,websiteUri,regularHours,metadata,profile'
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


def get_performance_metrics(credentials, location_name, start_date, end_date):
    """Get performance metrics for a date range"""
    headers = {
        'Authorization': f'Bearer {credentials.token}',
        'Content-Type': 'application/json'
    }
    
    location_id = location_name.split('/')[-1]
    url = f'https://businessprofileperformance.googleapis.com/v1/locations/{location_id}:fetchMultiDailyMetricsTimeSeries'
    
    metrics = [
        'BUSINESS_IMPRESSIONS_DESKTOP_MAPS',
        'BUSINESS_IMPRESSIONS_DESKTOP_SEARCH',
        'BUSINESS_IMPRESSIONS_MOBILE_MAPS',
        'BUSINESS_IMPRESSIONS_MOBILE_SEARCH',
        'BUSINESS_CONVERSATIONS',
        'BUSINESS_DIRECTION_REQUESTS',
        'CALL_CLICKS',
        'WEBSITE_CLICKS',
        'BUSINESS_BOOKINGS',
        'BUSINESS_FOOD_ORDERS'
    ]
    
    params = {
        'dailyMetrics': metrics,
        'dailyRange.start_date.year': start_date.year,
        'dailyRange.start_date.month': start_date.month,
        'dailyRange.start_date.day': start_date.day,
        'dailyRange.end_date.year': end_date.year,
        'dailyRange.end_date.month': end_date.month,
        'dailyRange.end_date.day': end_date.day
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching metrics for {location_name}: {e}")
        return None


def process_metrics_data_daily(metrics_response):
    """Process metrics response to return DAILY data"""
    if not metrics_response:
        return []
    
    daily_data = {}
    
    for multi_series in metrics_response.get('multiDailyMetricTimeSeries', []):
        for daily_series in multi_series.get('dailyMetricTimeSeries', []):
            metric = daily_series.get('dailyMetric', 'UNKNOWN')
            time_series = daily_series.get('timeSeries', {})
            dated_values = time_series.get('datedValues', [])
            
            for dated_value in dated_values:
                date_obj = dated_value.get('date', {})
                date_str = f"{date_obj.get('year')}-{date_obj.get('month'):02d}-{date_obj.get('day'):02d}"
                
                if date_str not in daily_data:
                    daily_data[date_str] = {}
                
                value = int(dated_value.get('value', 0))
                daily_data[date_str][metric] = value
    
    daily_records = []
    for date_str, metrics in daily_data.items():
        record = {'date': date_str}
        record.update(metrics)
        daily_records.append(record)
    
    return daily_records


def transform_to_bigquery_rows(all_daily_data):
    """Transform raw data to BigQuery-ready rows"""
    rows = []
    
    for record in all_daily_data:
        # Parse date
        date_obj = datetime.strptime(record['date'], '%Y-%m-%d')
        
        row = {
            'date': record['date'],
            'day_of_week': date_obj.strftime('%A'),
            'week_number': date_obj.isocalendar()[1],
            'month': date_obj.month,
            'month_name': date_obj.strftime('%B'),
            'year': date_obj.year,
            
            # Location info
            'location_name': record['location_name'],
            'title': record['title'],
            'phone': record.get('phone', ''),
            'website': record.get('website', ''),
            'address': record.get('address', ''),
            'maps_url': record.get('maps_url', ''),
            
            # Impressions
            'impressions_desktop_maps': record.get('BUSINESS_IMPRESSIONS_DESKTOP_MAPS', 0),
            'impressions_desktop_search': record.get('BUSINESS_IMPRESSIONS_DESKTOP_SEARCH', 0),
            'impressions_mobile_maps': record.get('BUSINESS_IMPRESSIONS_MOBILE_MAPS', 0),
            'impressions_mobile_search': record.get('BUSINESS_IMPRESSIONS_MOBILE_SEARCH', 0),
            
            # Actions
            'conversations': record.get('BUSINESS_CONVERSATIONS', 0),
            'direction_requests': record.get('BUSINESS_DIRECTION_REQUESTS', 0),
            'call_clicks': record.get('CALL_CLICKS', 0),
            'website_clicks': record.get('WEBSITE_CLICKS', 0),
            'bookings': record.get('BUSINESS_BOOKINGS', 0),
            'food_orders': record.get('BUSINESS_FOOD_ORDERS', 0),
            
            # Calculated fields
            'total_impressions': (
                record.get('BUSINESS_IMPRESSIONS_DESKTOP_MAPS', 0) +
                record.get('BUSINESS_IMPRESSIONS_DESKTOP_SEARCH', 0) +
                record.get('BUSINESS_IMPRESSIONS_MOBILE_MAPS', 0) +
                record.get('BUSINESS_IMPRESSIONS_MOBILE_SEARCH', 0)
            ),
            'total_actions': (
                record.get('WEBSITE_CLICKS', 0) +
                record.get('CALL_CLICKS', 0) +
                record.get('BUSINESS_DIRECTION_REQUESTS', 0) +
                record.get('BUSINESS_CONVERSATIONS', 0)
            ),
            
            # Metadata
            'data_fetched_at': datetime.utcnow().isoformat()
        }
        
        rows.append(row)
    
    return rows


def write_to_bigquery(rows):
    """Write rows to BigQuery with upsert logic"""
    if not rows:
        logger.warning("No rows to write to BigQuery")
        return
    
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Configure write disposition to replace data for the date range
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    
    try:
        # First, delete existing records for this date range
        dates = set(row['date'] for row in rows)
        date_list = "', '".join(dates)
        
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE date IN ('{date_list}')
        """
        
        logger.info(f"Deleting existing records for {len(dates)} dates")
        client.query(delete_query).result()
        
        # Insert new records
        logger.info(f"Inserting {len(rows)} new records")
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
    logger.info("Starting Daily Impressions Data Collection")
    logger.info("="*70)
    
    # Set date range - Last 90 days, ending 3 days ago (API delay)
    end_date = datetime.now() - timedelta(days=3)
    start_date = end_date - timedelta(days=90)
    
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
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
        
        # Collect data for all locations
        all_daily_data = []
        
        for idx, location in enumerate(locations, 1):
            location_name = location['name']
            location_title = location.get('title', 'N/A')
            
            logger.info(f"[{idx}/{len(locations)}] Processing: {location_title}")
            
            # Get metrics
            metrics = get_performance_metrics(credentials, location_name, start_date, end_date)
            daily_records = process_metrics_data_daily(metrics) if metrics else []
            
            if daily_records:
                logger.info(f"  ✅ Got {len(daily_records)} days of data")
                
                # Add location info to each record
                for record in daily_records:
                    record['location_name'] = location_name
                    record['title'] = location_title
                    record['address'] = str(location.get('storefrontAddress', {}))
                    record['phone'] = location.get('phoneNumbers', {}).get('primaryPhone', 'N/A')
                    record['website'] = location.get('websiteUri', 'N/A')
                    record['maps_url'] = location.get('metadata', {}).get('mapsUrl', 'N/A')
                    
                    all_daily_data.append(record)
            else:
                logger.warning(f"  ⚠️ No metrics data for {location_title}")
        
        # Transform and write to BigQuery
        logger.info(f"Transforming {len(all_daily_data)} records...")
        rows = transform_to_bigquery_rows(all_daily_data)
        
        logger.info("Writing to BigQuery...")
        write_to_bigquery(rows)
        
        logger.info("="*70)
        logger.info("✅ Data collection complete!")
        logger.info(f"Total records written: {len(rows)}")
        logger.info("="*70)
        
        return {
            "status": "success",
            "records_written": len(rows),
            "locations_processed": len(locations),
            "date_range": {
                "start": start_date.date().isoformat(),
                "end": end_date.date().isoformat()
            }
        }, 200
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}, 500


# For local testing
if __name__ == "__main__":
    result, status_code = main()
    print(f"\nResult ({status_code}): {result}")
