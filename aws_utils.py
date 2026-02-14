"""
AWS utilities for credential management
"""
import os
from logger_config import get_logger


def setup_aws_credentials(config):
    """Set up AWS credentials from environment variables"""
    logger = get_logger(__name__)
    
    # Read credentials from environment variables
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    region = os.environ.get("AWS_DEFAULT_REGION")
    
    if access_key:
        access_key = access_key.strip()
    if secret_key:
        secret_key = secret_key.strip()
    if region:
        region = region.strip()
    
    # Set environment variables if credentials are from env
    if access_key and secret_key:
        os.environ["AWS_ACCESS_KEY_ID"] = access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
        logger.debug("AWS credentials loaded from environment variables")
    
    if region:
        os.environ["AWS_DEFAULT_REGION"] = region


def check_aws_credentials():
    """Check if AWS credentials are available"""
    logger = get_logger(__name__)
    
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    
    if not access_key or not secret_key:
        logger.error("AWS credentials not found!")
        logger.error("Please add credentials to config.conf:")
        logger.error("  [AWS]")
        logger.error("  ACCESS_KEY_ID = your-access-key-id")
        logger.error("  SECRET_ACCESS_KEY = your-secret-access-key")
        return False
    
    logger.debug("AWS credentials configured")
    return True

