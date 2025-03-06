"""Address normalization and geocoding utilities."""

import re
import logging
import requests
from botocore.exceptions import ClientError
import boto3
from metrics import record_geocoding_success
from config import VARIATION_TYPES, HERE_GEOCODE_API, HERE_API_KEY_PARAM

# Configure logging
logger = logging.getLogger()

# Initialize AWS clients
ssm = boto3.client('ssm')

# Cache for HERE API key
_here_api_key = None

def get_here_api_key():
    """Retrieve HERE API key from Parameter Store with caching"""
    global _here_api_key
    
    if _here_api_key:
        return _here_api_key
        
    try:
        response = ssm.get_parameter(
            Name=HERE_API_KEY_PARAM,
            WithDecryption=True
        )
        _here_api_key = response['Parameter']['Value']
        return _here_api_key
    except ClientError as e:
        logger.error(f"Error retrieving HERE API key: {e}")
        return None

def normalize_address(address):
    """Normalize address format by standardizing abbreviations"""
    if not address:
        return address
    
    # Convert to uppercase for consistent processing
    addr = address.upper()
    
    # Standardize common abbreviations
    abbrev_map = {
        'STREET': 'ST',
        'AVENUE': 'AVE',
        'BOULEVARD': 'BLVD',
        'DRIVE': 'DR',
        'ROAD': 'RD',
        'LANE': 'LN',
        'COURT': 'CT',
        'CIRCLE': 'CIR',
        'PLACE': 'PL',
        'HIGHWAY': 'HWY',
        'PARKWAY': 'PKWY',
        'APARTMENT': 'APT',
        'SUITE': 'STE',
        'NORTH': 'N',
        'SOUTH': 'S',
        'EAST': 'E',
        'WEST': 'W',
        'NORTHEAST': 'NE',
        'NORTHWEST': 'NW',
        'SOUTHEAST': 'SE',
        'SOUTHWEST': 'SW'
    }
    
    for full, abbr in abbrev_map.items():
        # Replace full words with abbreviations, ensuring they're whole words
        addr = re.sub(r'\b' + full + r'\b', abbr, addr)
    
    # Convert back to title case for readability
    return addr.title()

def remove_unit(address):
    """Remove apartment/unit numbers from address"""
    if not address:
        return address
    
    # Remove apartment/unit designations
    addr = re.sub(r'\b(?:APT|UNIT|STE|#)\s*[\w-]+', '', address, flags=re.IGNORECASE)
    
    # Remove trailing commas and clean up extra spaces
    addr = re.sub(r',\s*$', '', addr)
    addr = re.sub(r'\s+', ' ', addr).strip()
    
    return addr

def remove_secondary(address):
    """Remove secondary address line (everything after comma)"""
    if not address:
        return address
    
    # Split on first comma and take only the first part
    parts = address.split(',', 1)
    return parts[0].strip()

def extract_street_city_state(address):
    """Extract just street, city, and state from address"""
    if not address:
        return address
    
    # Try to match pattern: street, city, state zip
    match = re.match(r'(.*?),\s*(.*?),\s*([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?$', address, re.IGNORECASE)
    if match:
        street, city, state = match.groups()
        return f"{street}, {city}, {state}"
    
    # If no match, return original address
    return address

def extract_street_zip(address):
    """Extract just street and ZIP code from address"""
    if not address:
        return address
    
    # Try to match street and ZIP
    street_match = re.match(r'(.*?),', address, re.IGNORECASE)
    zip_match = re.search(r'(\d{5}(?:-\d{4})?)', address)
    
    if street_match and zip_match:
        street = street_match.group(1).strip()
        zip_code = zip_match.group(1)
        return f"{street}, {zip_code}"
    
    # If no match, return original address
    return address

def geocode_address(address, api_key):
    """Geocode an address using HERE Geocoding API"""
    if not address or not api_key:
        return None
    
    try:
        # Encode address for URL
        encoded_address = requests.utils.quote(address)
        
        # Call HERE Geocoding API
        url = f"{HERE_GEOCODE_API}?q={encoded_address}&apiKey={api_key}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            # Check if we got any results
            if data.get('items') and len(data['items']) > 0:
                # Get coordinates from first result
                position = data['items'][0].get('position')
                if position:
                    return (position['lat'], position['lng'])
        
        logger.warning(f"Geocoding failed for address: {address}, status: {response.status_code}")
        return None
    except Exception as e:
        logger.error(f"Error geocoding address: {e}")
        return None

def geocode_with_fuzzy_matching(original_address):
    """Try multiple variations of the address to improve geocoding success rate"""
    if not original_address:
        return None
    
    api_key = get_here_api_key()
    if not api_key:
        logger.error("No HERE API key available for geocoding")
        return None
    
    # Define address variations to try
    variations = [
        original_address,  # Original address as provided
        normalize_address(original_address),  # Normalized abbreviations
        remove_unit(original_address),  # Remove apartment/unit numbers
        remove_secondary(original_address),  # Remove secondary address line
        extract_street_city_state(original_address),  # Just street, city, state
        extract_street_zip(original_address)  # Just street and ZIP
    ]
    
    # Get variation type names for metrics
    variation_types = [
        VARIATION_TYPES['ORIGINAL'],
        VARIATION_TYPES['NORMALIZED'],
        VARIATION_TYPES['NO_UNIT'],
        VARIATION_TYPES['NO_SECONDARY'],
        VARIATION_TYPES['STREET_CITY_STATE'],
        VARIATION_TYPES['STREET_ZIP']
    ]
    
    # Try each variation until one succeeds
    for i, addr_variation in enumerate(variations):
        if not addr_variation:
            continue
            
        logger.info(f"Trying address variation: {addr_variation}")
        
        # Geocode this variation
        coords = geocode_address(addr_variation, api_key)
        
        if coords:
            logger.info(f"Successfully geocoded with variation: {addr_variation}")
            record_geocoding_success(variation_types[i], True)
            return coords
        else:
            record_geocoding_success(variation_types[i], False)
    
    # If all variations fail
    logger.warning(f"All geocoding attempts failed for address: {original_address}")
    return None 