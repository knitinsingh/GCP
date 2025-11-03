"""
Google Business Profile - Location Status & Ratings Collector
Collects verification status, publishing status, and ratings data
"""

import requests
import logging
import os
from datetime import datetime
from google.cloud import bigquery
from google.auth import default
from google.auth.transport.requests import Request

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = os.environ.get('BQ_DATASET', 'google_business_profile')
TABLE_ID = os.environ.get('BQ_TABLE_STATUS', 'location_status')
PLACES_API_KEY = os.environ.get('PLACES_API_KEY')  # Google Places API key


def get_credentials():
    """Get Application Default Credentials with proper scopes"""
    credentials, project = default(scopes=[
        'https://www.googleapis.com/auth/business.manage'
    ])
    
    if not credentials.valid:
        credentials.refresh(Request())
    
    return credentials


def get_all_locations_with_status(credentials):
    """Get all locations with status information"""
    headers = {
        'Authorization': f'Bearer {credentials.token}',
        'Content-Type': 'application/json'
    }
    
    read_mask = 'name,title,metadata,storefrontAddress,phoneNumbers,websiteUri'
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


def get_rating_from_places_api(place_id, api_key):
    """Get rating using Google Places API"""
    if not api_key:
        logger.warning("Places API key not configured")
        return None
    
    url = 'https://maps.googleapis.com/maps/api/place/details/json'
    
    params = {
        'place_id': place_id,
        'fields': 'name,rating,user_ratings_total',
        'key': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('status') == 'OK':
            result = data.get('result', {})
            return {
                'rating': result.get('rating'),
                'review_count': result.get('user_ratings_total', 0),
                'name': result.get('name')
            }
        else:
            logger.warning(f"Places API returned status: {data.get('status')}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling Places API for {place_id}: {e}")
        return None


def determine_location_status(metadata):
    """Determine verification and publishing status from metadata"""
    has_voice_of_merchant = metadata.get('hasVoiceOfMerchant', False)
    has_maps_uri = bool(metadata.get('mapsUri'))
    
    # Verification status
    verification_status = "VERIFIED" if has_voice_of_merchant else "NOT VERIFIED"
    
    # Publishing status
    publish_status = "PUBLISHED" if has_maps_uri else "NOT PUBLISHED"
    
    # Overall status
    if has_voice_of_merchant and has_maps_uri:
        overall_status = "ACTIVE - Verified & Published"
    elif has_voice_of_merchant:
        overall_status = "CLAIMED - Not Yet Published"
    elif has_maps_uri:
        overall_status = "PUBLISHED - Not Claimed"
    else:
        overall_status = "INACTIVE - Not Claimed/Published"
    
    return {
        'verification_status': verification_status,
        'publish_status': publish_status,
        'overall_status': overall_status,
        'is_verified': has_voice_of_merchant,
        'is_published': has_maps_uri
    }


def transform_to_bigquery_rows(locations, api_key):
    """Transform location data to BigQuery-ready rows"""
    rows = []
    current_timestamp = datetime.utcnow()
    check_date = current_timestamp.date().isoformat()
    
    for idx, loc in enumerate(locations, 1):
        metadata = loc.get('metadata', {})
        
        # Get status information
        status_info = determine_location_status(metadata)
        
        title = loc.get('title', 'N/A')
        place_id = metadata.get('placeId')
        
        logger.info(f"[{idx}/{len(locations)}] Processing {title}")
        
        # Get rating data from Places API
        rating = None
        review_count = 0
        
        if place_id and api_key:
            rating_data = get_rating_from_places_api(place_id, api_key)
            
            if rating_data:
                rating = rating_data.get('rating')
                review_count = rating_data.get('review_count', 0)
                logger.info(f"  ✅ Rating: {rating}⭐ ({review_count} reviews)")
            else:
                logger.warning(f"  ⚠️ Could not fetch rating")
        else:
            logger.warning(f"  ⚠️ No Place ID or API key")
        
        # Build row
        row = {
            'check_date': check_date,
            'check_timestamp': current_timestamp.isoformat(),
            'title': title,
            'location_id': loc.get('name', 'N/A'),
            'phone': loc.get('phoneNumbers', {}).get('primaryPhone', ''),
            'website': loc.get('websiteUri', ''),
            'address': str(loc.get('storefrontAddress', {})),
            
            # Status Information
            'overall_status': status_info['overall_status'],
            'verification_status': status_info['verification_status'],
            'publish_status': status_info['publish_status'],
            'is_verified': status_info['is_verified'],
            'is_published': status_info['is_published'],
            
            # Rating Information
            'rating': rating if rating is not None else 0.0,
            'review_count': review_count,
            'has_rating': rating is not None,
            
            # Metadata
            'place_id': place_id if place_id else '',
            'maps_uri': metadata.get('mapsUri', ''),
            'new_review_uri': metadata.get('newReviewUri', ''),
            'can_delete': metadata.get('canDelete', False),
        }
        
        rows.append(row)
    
    return rows


def write_to_bigquery(rows):
    """Write rows to BigQuery with date-based replacement"""
    if not rows:
        logger.warning("No rows to write to BigQuery")
        return
    
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    try:
        # Delete existing records for today
        check_date = rows[0]['check_date']
        
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE check_date = '{check_date}'
        """
        
        logger.info(f"Deleting existing records for {check_date}")
        client.query(delete_query).result()
        
        # Insert new records
        logger.info(f"Inserting {len(rows)} new records")
        errors = client.insert_rows_json(table_ref, rows)
        
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
            raise Exception(f"Failed to insert rows: {errors}")
        
        logger.info(f"Successfully wrote {len(rows)} rows to BigQuery")
        
        # Log summary statistics
        verified_count = sum(1 for r in rows if r['is_verified'])
        published_count = sum(1 for r in rows if r['is_published'])
        with_ratings = sum(1 for r in rows if r['has_rating'])
        avg_rating = sum(r['rating'] for r in rows if r['has_rating']) / with_ratings if with_ratings > 0 else 0
        
        logger.info(f"Summary: {verified_count} verified, {published_count} published, {with_ratings} with ratings (avg: {avg_rating:.2f})")
        
    except Exception as e:
        logger.error(f"BigQuery write error: {e}")
        raise


def main(request=None):
    """
    Main function for Cloud Function
    Can be triggered by HTTP request or Cloud Scheduler
    """
    logger.info("="*70)
    logger.info("Starting Location Status & Ratings Collection")
    logger.info("="*70)
    
    logger.info(f"Target table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    
    # Check for Places API key
    if not PLACES_API_KEY:
        logger.warning("⚠️ PLACES_API_KEY not set - ratings will not be collected")
    
    try:
        # Get credentials
        logger.info("Authenticating...")
        credentials = get_credentials()
        
        # Get all locations with status
        logger.info("Fetching locations...")
        locations = get_all_locations_with_status(credentials)
        logger.info(f"Found {len(locations)} location(s)")
        
        if len(locations) == 0:
            logger.error("No locations found")
            return {"status": "error", "message": "No locations found"}, 400
        
        # Transform and get ratings
        logger.info("Processing locations and fetching ratings...")
        rows = transform_to_bigquery_rows(locations, PLACES_API_KEY)
        
        # Write to BigQuery
        logger.info("Writing to BigQuery...")
        write_to_bigquery(rows)
        
        # Calculate summary stats
        verified_count = sum(1 for r in rows if r['is_verified'])
        published_count = sum(1 for r in rows if r['is_published'])
        with_ratings = sum(1 for r in rows if r['has_rating'])
        avg_rating = sum(r['rating'] for r in rows if r['has_rating']) / with_ratings if with_ratings > 0 else 0
        total_reviews = sum(r['review_count'] for r in rows)
        
        logger.info("="*70)
        logger.info("✅ Location status & ratings collection complete!")
        logger.info(f"Total locations: {len(rows)}")
        logger.info(f"Verified: {verified_count}, Published: {published_count}")
        logger.info(f"With ratings: {with_ratings}, Avg rating: {avg_rating:.2f}⭐")
        logger.info(f"Total reviews: {total_reviews}")
        logger.info("="*70)
        
        return {
            "status": "success",
            "locations_processed": len(rows),
            "verified_count": verified_count,
            "published_count": published_count,
            "locations_with_ratings": with_ratings,
            "average_rating": round(avg_rating, 2),
            "total_reviews": total_reviews,
            "check_date": rows[0]['check_date']
        }, 200
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}, 500


# For local testing
if __name__ == "__main__":
    result, status_code = main()
    print(f"\nResult ({status_code}): {result}")
