"""DynamoDB interaction utilities."""

import logging
import boto3
import sys
from config import LOCKSMITH_BASE_ADDRESSES
from travel_time import calculate_travel_time_with_coords
from address_utils import geocode_with_fuzzy_matching
import time
from decimal import Decimal

# Import the global company variable
# We need to import the module itself to access the variable
import lambda_function

# Configure logging
logger = logging.getLogger()

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

def get_locksmiths(table_name):
    """Get all locksmith records from the specified DynamoDB table"""
    table = dynamodb.Table(table_name)
    
    try:
        response = table.scan()
        locksmiths = response.get('Items', [])
        logger.info(f"Retrieved {len(locksmiths)} locksmiths from table {table_name}")
        return locksmiths
    except Exception as e:
        logger.error(f"Error scanning DynamoDB table {table_name}: {e}")
        return []

def calculate_locksmith_eta(locksmith, new_job_address, new_job_coords=None):
    """Calculate ETA for a locksmith to reach a new job"""
    locksmith_id = locksmith.get('locksmithId', 'unknown')
    
    # Use the global company name
    company_name = lambda_function.CURRENT_COMPANY
    
    # Check if locksmith has jobs in queue
    job_queue = locksmith.get('jobQueue', [])
    
    if not job_queue:
        # If locksmith has no jobs, use base_address
        if 'base_address' in locksmith:
            # Use existing base_address field
            base_address = locksmith['base_address']
            current_location_address = base_address['address']
            current_location_coords = (base_address['coords'][0], base_address['coords'][1]) if base_address.get('coords') else None
        else:
            # Need to initialize base_address field
            # Get base address from config using company name
            base_address = LOCKSMITH_BASE_ADDRESSES.get(company_name)
            if not base_address:
                logger.warning(f"No base address found for company {company_name}")
                return float('inf'), 0  # Return infinity for ETA and 0 for travel time
            
            # Geocode the address
            coords = geocode_with_fuzzy_matching(base_address)
            current_location_address = base_address
            current_location_coords = coords
            
            if coords:
                # Create and store the base_address field
                base_address = {
                    'address': base_address,
                    'coords': coords
                }
                
                # Update the DynamoDB record
                try:
                    table_name = lambda_function.COMPANY_TABLE_MAPPING.get(company_name)
                    table = dynamodb.Table(table_name)
                    table.update_item(
                        Key={'locksmithId': locksmith_id},
                        UpdateExpression="set base_address = :b",
                        ExpressionAttributeValues={':b': base_address}
                    )
                    logger.info(f"Initialized base_address field for locksmith {locksmith_id} with company {company_name}")
                except Exception as e:
                    logger.error(f"Error updating base_address field: {e}")
        
        # Calculate travel time from base address to new job
        travel_time = calculate_travel_time_with_coords(
            current_location_address, 
            new_job_address,
            current_location_coords,
            new_job_coords
        )
        
        # No workload since queue is empty
        workload_minutes = 0
        
    else:
        # Calculate workload from jobs in queue
        workload_minutes = 0
        
        # Process each job in the queue
        for i, job in enumerate(job_queue):
            # Add estimated time for the job
            workload_minutes += float(job.get('estimatedTime', 0))
            
            # Add travel time only if not arrived yet
            if not job.get('arrived', False):
                workload_minutes += float(job.get('travelTime', 0))
        
        # Get the last job in the queue to calculate travel time to new job
        last_job = job_queue[-1]
        last_job_address = last_job.get('address')
        
        # Get coordinates from latitude/longitude fields if available
        last_job_coords = None
        if 'latitude' in last_job and 'longitude' in last_job:
            last_job_coords = (float(last_job['latitude']), float(last_job['longitude']))
        
        # Calculate travel time from last job to new job
        travel_time = calculate_travel_time_with_coords(
            last_job_address, 
            new_job_address,
            last_job_coords,
            new_job_coords
        )
    
    # Total ETA is workload + travel time
    eta = workload_minutes + travel_time
    
    logger.info(f"Locksmith {locksmith_id}: workload={workload_minutes}min, travel={travel_time}min, total ETA={eta}min")
    
    return eta, travel_time

def find_earliest_locksmith(locksmiths, new_job_address):
    """Find the locksmith with the earliest ETA for a new job"""
    if not locksmiths:
        return None, float('inf')
    
    # Get the company name from the global variable
    company_name = lambda_function.CURRENT_COMPANY
    
    # Geocode the new job address once
    new_job_coords = geocode_with_fuzzy_matching(new_job_address)
    if new_job_coords:
        logger.info(f"Geocoded new job address to coordinates: {new_job_coords}")
    else:
        logger.warning(f"Could not geocode new job address: {new_job_address}")
    
    earliest_eta = float('inf')
    earliest_locksmith_id = None
    earliest_travel_time = 0
    locksmith_count = 0
    
    for locksmith in locksmiths:
        eta, travel_time = calculate_locksmith_eta(locksmith, new_job_address, new_job_coords)
        locksmith_count += 1
        
        if eta < earliest_eta:
            earliest_eta = eta
            earliest_locksmith_id = locksmith.get('locksmithId')
            earliest_travel_time = travel_time
    
    logger.info(f"Processed {locksmith_count} locksmiths")
    
    # Update the NextAvailableCache table
    update_next_available_cache(company_name, earliest_locksmith_id, earliest_travel_time, new_job_coords, new_job_address)
    
    return earliest_locksmith_id, earliest_eta

def update_next_available_cache(company_name, locksmith_id, travel_time, new_job_coords=None, new_job_address=None):
    """Update the NextAvailableCache table with the latest availability information"""
    try:
        table = dynamodb.Table('NextAvailableCache')
        
        # Set TTL for 5 minutes (300 seconds)
        ttl = int(time.time()) + 300
        
        # Create the base item
        cache_item = {
            'companyName': company_name,
            'locksmithId': locksmith_id,
            'travelTime': int(travel_time),
            'ttl': ttl
        }
        
        # Add job address if available
        if new_job_address:
            cache_item['jobAddress'] = new_job_address
        
        # Add coordinates if available - convert to Decimal for DynamoDB
        if new_job_coords:
            cache_item['latitude'] = Decimal(str(new_job_coords[0]))
            cache_item['longitude'] = Decimal(str(new_job_coords[1]))
        
        # Update the cache
        table.put_item(Item=cache_item)
        
        logger.info(f"Updated NextAvailableCache for company {company_name}: locksmith={locksmith_id}, travelTime={int(travel_time)}, address={new_job_address}, coords={new_job_coords}")
    except Exception as e:
        logger.error(f"Error updating NextAvailableCache: {e}") 