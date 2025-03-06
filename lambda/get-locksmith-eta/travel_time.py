"""Travel time calculation utilities."""

import math
import logging
import requests
from botocore.exceptions import ClientError
import boto3
from address_utils import geocode_with_fuzzy_matching, get_here_api_key
from config import EARTH_RADIUS_KM, AVG_SPEED_KMH, ROAD_NETWORK_MULTIPLIER, HERE_ROUTING_API

# Configure logging
logger = logging.getLogger()

def calculate_travel_time_with_coords(origin_address, dest_address, origin_coords=None, dest_coords=None):
    """Calculate travel time using coordinates when available"""
    api_key = get_here_api_key()
    
    # Use stored coordinates if available
    if origin_coords:
        logger.info(f"Using stored coordinates for origin: {origin_coords}")
    else:
        # Geocode origin address
        origin_coords = geocode_with_fuzzy_matching(origin_address)
        if not origin_coords:
            logger.warning(f"Could not geocode origin address: {origin_address}")
            return calculate_travel_time_fallback(origin_address, dest_address)
    
    if dest_coords:
        logger.info(f"Using stored coordinates for destination: {dest_coords}")
    else:
        # Geocode destination address
        dest_coords = geocode_with_fuzzy_matching(dest_address)
        if not dest_coords:
            logger.warning(f"Could not geocode destination address: {dest_address}")
            return calculate_travel_time_fallback(origin_address, dest_address)
    
    # If we have both coordinates and API key, use HERE Routing API
    if api_key:
        try:
            # Format coordinates for HERE API
            origin_str = f"{origin_coords[0]},{origin_coords[1]}"
            dest_str = f"{dest_coords[0]},{dest_coords[1]}"
            
            logger.info(f"Calling HERE Routing API with origin: {origin_str}, destination: {dest_str}")
            
            # Call HERE Routing API
            url = f"{HERE_ROUTING_API}?transportMode=car&origin={origin_str}&destination={dest_str}&return=summary&apikey={api_key}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                # Extract travel time in seconds from the response
                travel_time_seconds = data['routes'][0]['sections'][0]['summary']['duration']
                travel_time_minutes = math.ceil(travel_time_seconds / 60)
                
                logger.info(f"HERE API travel time: {origin_address} to {dest_address} = {travel_time_minutes} minutes")
                return travel_time_minutes
            else:
                logger.warning(f"HERE Routing API returned status code {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Error calling HERE Routing API: {e}")
    
    # Fallback to haversine calculation if API call fails
    return calculate_travel_time_fallback(origin_address, dest_address, origin_coords, dest_coords)

def calculate_travel_time_fallback(origin_address, dest_address, origin_coords=None, dest_coords=None):
    """Calculate travel time using haversine formula as fallback"""
    # If we don't have coordinates, try to geocode
    if not origin_coords:
        origin_coords = geocode_with_fuzzy_matching(origin_address)
        if not origin_coords:
            logger.warning(f"Could not geocode origin address for fallback: {origin_address}")
            return 30  # Default travel time if geocoding fails
    
    if not dest_coords:
        dest_coords = geocode_with_fuzzy_matching(dest_address)
        if not dest_coords:
            logger.warning(f"Could not geocode destination address for fallback: {dest_address}")
            return 30  # Default travel time if geocoding fails
    
    # Calculate haversine distance
    lat1, lon1 = origin_coords
    lat2, lon2 = dest_coords
    
    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Haversine formula
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance_km = EARTH_RADIUS_KM * c
    
    # Apply road network multiplier to account for non-direct routes
    road_distance_km = distance_km * ROAD_NETWORK_MULTIPLIER
    
    # Calculate travel time in minutes
    travel_time_hours = road_distance_km / AVG_SPEED_KMH
    travel_time_minutes = math.ceil(travel_time_hours * 60)
    
    # Log that fallback was used
    logger.info(f"Fallback calculation used: {origin_address} to {dest_address} = {travel_time_minutes} minutes")
    
    return travel_time_minutes 