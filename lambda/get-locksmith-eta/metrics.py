"""Metrics recording utilities."""

import json
import logging
import time
import os
import boto3
from datetime import datetime
from config import METRICS_TABLE_NAME

# Configure logging
logger = logging.getLogger()

# Global variable to collect metrics during execution
_metrics_to_record = []

def record_geocoding_success(variation_type, success):
    """Queue metrics to be recorded at the end of the function"""
    global _metrics_to_record
    
    # Add the metric to our in-memory collection
    _metrics_to_record.append({
        'variation_type': variation_type,
        'success': success,
        'timestamp': datetime.now().isoformat()
    })
    
    logger.info(f"Queued metrics for variation type {variation_type}: success={success}")

def flush_metrics(context=None):
    """Record all queued metrics using a true fire-and-forget approach"""
    global _metrics_to_record
    
    if not _metrics_to_record:
        return
    
    # If context is provided and we're close to timeout, log a warning
    if context and context.get_remaining_time_in_millis() < 100:  # Less than 100ms left
        logger.warning(f"Close to timeout, dropping {len(_metrics_to_record)} metrics")
        _metrics_to_record = []
        return
        
    try:
        # Create a copy of the metrics to record
        metrics_to_flush = _metrics_to_record.copy()
        _metrics_to_record = []
        
        # Start a thread to handle the invocation without waiting for any response
        import threading
        
        def invoke_async():
            try:
                lambda_client = boto3.client('lambda')
                
                # Create a payload with the metrics
                payload = {
                    'action': 'record_metrics_batch',
                    'metrics': metrics_to_flush
                }
                
                # Get the current function name from environment variables
                function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'get-locksmith-eta')
                
                # Invoke the Lambda function asynchronously and don't wait for any response
                lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType='Event',  # Asynchronous invocation
                    Payload=json.dumps(payload)
                )
            except Exception:
                pass  # Truly fire-and-forget
        
        # Start the thread and return immediately
        thread = threading.Thread(target=invoke_async)
        thread.daemon = True  # Allow the Lambda to exit even if thread is running
        thread.start()
        
        # Log that we've started the thread, but don't wait for any results
        logger.info(f"Started fire-and-forget thread to flush {len(metrics_to_flush)} metrics")
        
    except Exception as e:
        logger.error(f"Error setting up metrics flush: {e}")

def process_metrics_batch(metrics):
    """Process a batch of metrics by writing to DynamoDB"""
    if not metrics:
        return {'statusCode': 200, 'body': 'No metrics to process'}
    
    logger.info(f"Processing {len(metrics)} metrics")
    
    try:
        # Initialize DynamoDB client
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('FuzzyAddressMetrics')
        
        # Process DynamoDB metrics using batch writer
        with table.batch_writer() as batch:
            for metric in metrics:
                batch.put_item(
                    Item={
                        'timestamp': metric['timestamp'],
                        'variationType': metric['variation_type'],
                        'success': metric['success'],
                        'ttl': int(time.time()) + 30 * 24 * 60 * 60  # 30 days TTL
                    }
                )
        
        logger.info(f"Processed {len(metrics)} metrics")
        return {'statusCode': 200, 'body': f'Processed {len(metrics)} metrics'}
        
    except Exception as e:
        logger.error(f"Error processing metrics batch: {e}")
        return {'statusCode': 500, 'body': f'Error processing metrics: {str(e)}'} 