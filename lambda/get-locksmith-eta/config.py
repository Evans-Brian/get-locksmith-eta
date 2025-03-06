"""Configuration and constants for the locksmith ETA Lambda function."""

# Base addresses for locksmith companies
LOCKSMITH_BASE_ADDRESSES = {
    'QuickFix': "1614 10th St S, Arlington, VA 22204",
    # Add more locksmith companies as needed
}

# Map company names to their respective DynamoDB table names
COMPANY_TABLE_MAPPING = {
    'QuickFix': 'QuickFixLocksmithJobs',
    # Add more companies as needed
}

# Constants for fallback calculation
EARTH_RADIUS_KM = 6371  # Earth radius in kilometers
AVG_SPEED_KMH = 48.28   # 30 mph in km/h
ROAD_NETWORK_MULTIPLIER = 1.4  # Typical multiplier to account for road networks

# Address variation types for metrics
VARIATION_TYPES = {
    'ORIGINAL': 'original',
    'NORMALIZED': 'normalized',
    'NO_UNIT': 'no_unit',
    'NO_SECONDARY': 'no_secondary',
    'STREET_CITY_STATE': 'street_city_state',
    'STREET_ZIP': 'street_zip'
}

# HERE API configuration
HERE_API_KEY_PARAM = '/locksmith-eta/here-api-key'
HERE_GEOCODE_API = "https://geocode.search.hereapi.com/v1/geocode"
HERE_ROUTING_API = "https://router.hereapi.com/v8/routes"

# Metrics configuration
METRICS_TABLE_NAME = 'FuzzyAddressMetrics'
METRICS_NAMESPACE = 'LocksmithETA' 