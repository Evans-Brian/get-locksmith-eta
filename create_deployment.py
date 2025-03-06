import boto3
import os
import zipfile
import io
import subprocess
import shutil

def create_deployment_package(function_name):
    """Create a deployment package for the Lambda function"""
    # Create a temporary directory for the package
    if not os.path.exists('package'):
        os.makedirs('package')
    
    # Copy all Python files from the function directory
    function_dir = f'lambda/{function_name}'
    for file in os.listdir(function_dir):
        if file.endswith('.py'):
            shutil.copy(f'{function_dir}/{file}', f'package/{file}')
    
    # Install dependencies
    subprocess.check_call([
        'pip', 'install', 
        '-r', f'{function_dir}/requirements.txt', 
        '--target', 'package/'
    ])
    
    # Create a zip file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk('package'):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, 'package')
                zip_file.write(file_path, arcname)
    
    # Clean up
    shutil.rmtree('package')
    
    return zip_buffer.getvalue()

def update_lambda_function(function_name, region='us-east-2'):
    """Update an existing Lambda function"""
    lambda_client = boto3.client('lambda', region_name=region)
    
    # Create deployment package
    deployment_package = create_deployment_package(function_name)
    
    # Update the Lambda function
    response = lambda_client.update_function_code(
        FunctionName=function_name,
        ZipFile=deployment_package,
        Publish=True
    )
    
    print(f"Updated Lambda function: {function_name}")
    print(f"Version: {response['Version']}")
    print(f"ARN: {response['FunctionArn']}")

if __name__ == "__main__":
    update_lambda_function('get-locksmith-eta') 