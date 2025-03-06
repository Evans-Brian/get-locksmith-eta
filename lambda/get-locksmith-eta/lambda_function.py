"""Main Lambda handler for the locksmith ETA function."""

import json
import logging
import decimal
import time
from config import COMPANY_TABLE_MAPPING
from dynamo_utils import get_locksmiths, find_earliest_locksmith
from metrics import flush_metrics, process_metrics_batch

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global variable to store the current company name
CURRENT_COMPANY = None

# Helper class to convert Decimal to float for JSON serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def extract_parameters_from_event(event):
    """Extract address and company parameters from different event formats"""
    # Check if this is a metrics processing event
    if event.get('action') == 'record_metrics_batch':
        return None, None
        
    # Check if this is an API Gateway event
    if event.get('requestContext') and event.get('body'):
        logger.info("Processing API Gateway event format")
        try:
            # Parse the body
            body = json.loads(event['body'])
            
            # Extract parameters from the tool call arguments
            if 'args' in body:
                # Direct args format
                return body['args'].get('address'), body['args'].get('company')
            elif 'call' in body and 'transcript_with_tool_calls' in body['call']:
                # Look for the tool call invocation
                for item in body['call']['transcript_with_tool_calls']:
                    if item.get('role') == 'tool_call_invocation' and item.get('name') == 'get_eta':
                        args = json.loads(item.get('arguments', '{}'))
                        return args.get('address'), args.get('company')
            
            logger.warning(f"Could not extract parameters from API Gateway event: {body}")
            return None, None
        except Exception as e:
            logger.error(f"Error parsing API Gateway event: {e}")
            return None, None
    
    # Standard direct Lambda invocation format
    logger.info("Processing direct Lambda invocation format")
    return event.get('address'), event.get('company')

def lambda_handler(event, context):
    """Main Lambda handler function"""
    global CURRENT_COMPANY
    
    # Check if this is a metrics processing invocation
    if event.get('action') == 'record_metrics_batch':
        return process_metrics_batch(event.get('metrics', []))
    
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Extract input parameters based on event format
        new_job_address, company_name = extract_parameters_from_event(event)
        
        # Validate required parameters
        if not new_job_address:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameter: address'})
            }
        
        if not company_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameter: company'})
            }
        
        # Set the global company name
        CURRENT_COMPANY = company_name
        
        # Get table name from company mapping
        table_name = COMPANY_TABLE_MAPPING.get(company_name)
        if not table_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f'Unknown company: {company_name}'})
            }
        
        logger.info(f"Processing request for company: {company_name}, table: {table_name}, address: {new_job_address}")
        
        # Get all locksmiths from the table
        locksmiths = get_locksmiths(table_name)
        
        if not locksmiths:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No locksmiths found in the table'})
            }
        
        # Find locksmith with earliest ETA (this now also updates the cache)
        earliest_locksmith_id, earliest_eta = find_earliest_locksmith(locksmiths, new_job_address)
        
        # Prepare the response
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'locksmithId': earliest_locksmith_id,
                'etaMinutes': earliest_eta
            }, cls=DecimalEncoder)
        }
        
        # Flush metrics asynchronously before returning
        flush_metrics(context)
        
        return result
    
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'})
        } 