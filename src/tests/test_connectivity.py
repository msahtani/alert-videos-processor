"""
Connectivity testing utilities for API and S3
"""
import os
import tempfile
from datetime import datetime
from typing import Tuple, Optional
from src.utils.logger_config import get_logger, PerformanceLogger
from src.core.api_client import APIClient
from src.core.s3_uploader import S3Uploader


def test_api_connectivity(api_client: APIClient) -> Tuple[bool, str]:
    """
    Test API connectivity by fetching global settings
    
    Args:
        api_client: APIClient instance
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    logger = get_logger(__name__)
    
    try:
        logger.info("Testing API connectivity: Fetching global settings...")
        with PerformanceLogger(logger, "test_api_global_settings"):
            global_settings = api_client.get_global_settings()
        
        if global_settings:
            logger.info("✓ API connectivity test passed: Global settings fetched successfully")
            return True, "API connectivity test passed: Global settings fetched successfully"
        else:
            logger.warning("⚠ API connectivity test: Global settings fetch returned None (may be expected)")
            return True, "API connectivity test: Global settings fetch returned None (may be expected)"
            
    except Exception as e:
        logger.error(f"✗ API connectivity test failed: {e}", exc_info=True)
        return False, f"API connectivity test failed: {e}"


def test_alerts_api(api_client: APIClient, date: str) -> Tuple[bool, str]:
    """
    Test alerts API by fetching alerts for a specific date
    
    Args:
        api_client: APIClient instance
        date: Date in ISO format (e.g., 2025-12-10T00:00:00Z)
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    logger = get_logger(__name__)
    
    try:
        logger.info(f"Testing alerts API: Fetching alerts for date {date}...")
        with PerformanceLogger(logger, "test_alerts_api", date=date):
            alerts = api_client.get_alerts(date)
        
        alert_count = len(alerts) if alerts else 0
        logger.info(f"✓ Alerts API test passed: Retrieved {alert_count} alert(s) for date {date}")
        return True, f"Alerts API test passed: Retrieved {alert_count} alert(s) for date {date}"
            
    except Exception as e:
        logger.error(f"✗ Alerts API test failed: {e}", exc_info=True)
        return False, f"Alerts API test failed: {e}"


def test_s3_connectivity(s3_uploader: S3Uploader) -> Tuple[bool, str]:
    """
    Test S3 connectivity by listing bucket and uploading a test file
    
    Args:
        s3_uploader: S3Uploader instance
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    logger = get_logger(__name__)
    
    try:
        # Test 1: List bucket (head bucket operation)
        logger.info(f"Testing S3 connectivity: Listing bucket '{s3_uploader.bucket}'...")
        with PerformanceLogger(logger, "test_s3_list_bucket"):
            s3_uploader.s3_client.head_bucket(Bucket=s3_uploader.bucket)
        logger.info("✓ S3 bucket access verified")
        
        # Test 2: Upload a small test file
        logger.info("Testing S3 connectivity: Uploading test file...")
        test_content = f"Test file created at {datetime.now().isoformat()}"
        test_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as test_file:
            test_file.write(test_content)
            test_file_path = test_file.name
        
        try:
            with PerformanceLogger(logger, "test_s3_upload"):
                test_key = f"{s3_uploader.prefix}test_{test_timestamp}.txt"
                s3_uploader.s3_client.upload_file(
                    test_file_path,
                    s3_uploader.bucket,
                    test_key,
                    ExtraArgs={'ContentType': 'text/plain'}
                )
            logger.info(f"✓ Test file uploaded successfully to s3://{s3_uploader.bucket}/{test_key}")
            
            # Test 3: Delete the test file
            logger.info("Testing S3 connectivity: Deleting test file...")
            with PerformanceLogger(logger, "test_s3_delete"):
                s3_uploader.s3_client.delete_object(Bucket=s3_uploader.bucket, Key=test_key)
            logger.info("✓ Test file deleted successfully")
            
            logger.info("✓ S3 connectivity test passed: Upload and delete operations successful")
            return True, "S3 connectivity test passed: Upload and delete operations successful"
            
        finally:
            # Clean up local test file
            try:
                os.remove(test_file_path)
            except Exception:
                pass
                
    except Exception as e:
        logger.error(f"✗ S3 connectivity test failed: {e}", exc_info=True)
        return False, f"S3 connectivity test failed: {e}"


def run_connectivity_tests(api_client: APIClient, s3_uploader: S3Uploader, test_date: Optional[str] = None) -> bool:
    """
    Run all connectivity tests
    
    Args:
        api_client: APIClient instance
        s3_uploader: S3Uploader instance
        test_date: Optional date to test alerts API (ISO format, e.g., 2025-12-10T00:00:00Z)
        
    Returns:
        True if all tests passed, False otherwise
    """
    logger = get_logger(__name__)
    
    logger.info("=" * 60)
    logger.info("Running connectivity tests...")
    logger.info("=" * 60)
    
    all_passed = True
    
    # Test API connectivity (global settings)
    api_success, api_message = test_api_connectivity(api_client)
    if not api_success:
        all_passed = False
    
    print(f"\nAPI Test (Global Settings): {api_message}")
    
    # Test alerts API if date is provided
    if test_date:
        alerts_success, alerts_message = test_alerts_api(api_client, test_date)
        if not alerts_success:
            all_passed = False
        print(f"API Test (Alerts for {test_date}): {alerts_message}")
    
    # Test S3 connectivity
    s3_success, s3_message = test_s3_connectivity(s3_uploader)
    if not s3_success:
        all_passed = False
    
    print(f"S3 Test: {s3_message}")
    
    logger.info("=" * 60)
    if all_passed:
        logger.info("✓ All connectivity tests passed!")
        print("\n✓ All connectivity tests passed!")
    else:
        logger.error("✗ Some connectivity tests failed!")
        print("\n✗ Some connectivity tests failed!")
    logger.info("=" * 60)
    
    return all_passed

