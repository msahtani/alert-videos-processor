"""
Main orchestrator script for processing alerts and extracting video clips
"""
import argparse
import logging
import os
import sys
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Optional

from api_client import APIClient
from clip_extractor import ClipExtractor
from s3_uploader import S3Uploader
from email_sender import EmailSender
from logger_config import setup_logging, get_logger, PerformanceLogger

from device_utils import get_device_id
from status_manager import read_status_file, write_status_file
from aws_utils import setup_aws_credentials, check_aws_credentials
from config_manager import load_config, parse_config
from progress_utils import LoggingTqdm
from cleanup_utils import cleanup_recordings
from alert_processor import process_alert
from test_connectivity import run_connectivity_tests


def setup_resume_logger(log_dir: str) -> logging.Logger:
    """Setup resume log file for progress bar updates"""
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    resume_log_file = log_path / "alert_processor_resume.log"
    resume_log_handler = logging.FileHandler(resume_log_file, encoding="utf-8")
    resume_log_handler.setLevel(logging.INFO)
    resume_log_formatter = logging.Formatter(
        fmt="%(asctime)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    resume_log_handler.setFormatter(resume_log_formatter)
    resume_logger = logging.getLogger("resume")
    resume_logger.setLevel(logging.INFO)
    resume_logger.addHandler(resume_log_handler)
    resume_logger.propagate = False
    return resume_logger


def get_fetch_date(date_cursor: Optional[int]) -> str:
    """Calculate fetch date based on date cursor"""
    if date_cursor is not None:
        current_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        target_date = current_date + timedelta(days=date_cursor)
        return target_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')


def initialize_email_sender(config, logger):
    """Initialize email sender if enabled"""
    email_enabled = config.get("email_enabled", False)
    if not email_enabled:
        return None
    
    try:
        email_sender = EmailSender(
            from_email=config["from_email"],
            to_emails=config["to_emails"],
            use_tls=config["use_tls"]
        )
        return email_sender
    except Exception as e:
        logger.warning(f"Failed to initialize email sender: {e}", exc_info=True)
        logger.warning("Continuing without email notifications...")
        return None


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Process alerts and extract video clips")
    parser.add_argument(
        "--date-cursor",
        type=int,
        default=None,
        help="Days offset from today (negative values for past dates). -1: yesterday, -2: 2 days ago, etc. If not provided, uses current date."
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging (DEBUG level)"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.conf",
        help="Path to config file (default: config.conf)"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test API and S3 connectivity and exit"
    )
    args = parser.parse_args()
    
    # Setup logging
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    log_dir = os.environ.get("LOG_DIR", "logs")
    json_logging = os.environ.get("JSON_LOGGING", "false").lower() == "true"
    
    setup_logging(
        log_level=log_level,
        log_dir=log_dir,
        log_file="alert_processor.log",
        json_logging=json_logging,
        verbose=args.verbose
    )
    
    # Setup resume logger
    resume_logger = setup_resume_logger(log_dir)
    
    # Get logger with correlation ID
    correlation_id = str(uuid.uuid4())
    logger = get_logger(__name__, {"correlation_id": correlation_id})
    
    # Get device ID early (needed for fetching global settings and creating APIClient)
    device_id = get_device_id()
    logger.info(f"Device ID: {device_id}", extra={"device_id": device_id})
    
    # Load config file first to get BASE_URL for APIClient
    try:
        config_obj = load_config(args.config)
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}", exc_info=True)
        sys.exit(1)
    
    # Get API base URL from config (needed to create APIClient)
    api_base_url = config_obj.get("API", "BASE_URL", fallback=None)
    if not api_base_url:
        logger.error("BASE_URL not found in config.conf! Please add BASE_URL to the [API] section")
        sys.exit(1)
    api_base_url = api_base_url.strip()
    alerts_endpoint = config_obj.get("API", "ALERTS_ENDPOINT").strip()
    secondary_video_endpoint = config_obj.get("API", "SECONDARY_VIDEO_ENDPOINT").strip()
    
    # Create APIClient early (needed for fetching global settings in parse_config)
    api_client = APIClient(
        base_url=api_base_url,
        alerts_endpoint=alerts_endpoint,
        secondary_video_endpoint=secondary_video_endpoint,
        device_id=device_id
    )
    
    # Parse configuration (this will fetch global settings using api_client)
    try:
        config = parse_config(config_obj, api_client)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}", exc_info=True)
        sys.exit(1)
    
    # Set up AWS credentials (may have been set from global settings)
    with PerformanceLogger(logger, "setup_aws_credentials"):
        setup_aws_credentials(config_obj)
    
    # Check AWS credentials
    if not check_aws_credentials():
        logger.error("AWS credentials are required for uploading processed clips to S3")
        sys.exit(1)
    
    # Replace {device-id} in the prefix
    s3_upload_prefix = config["s3_upload_prefix_template"].replace("{device-id}", device_id)
    
    # api_client already created above for fetching global settings
    
    # Log workflow configuration
    logger.info(f"Source: Loading video chunks from local directory '{config['local_source_dir']}'")
    logger.info(f"Destination: Uploading processed clips to S3 bucket '{config['s3_bucket']}/{s3_upload_prefix}'")
    
    # Initialize S3 uploader
    s3_uploader = S3Uploader(config["aws_region"], config["s3_bucket"], s3_upload_prefix)
    
    # Run connectivity tests if --test flag is set
    if args.test:
        # Calculate test date if --date-cursor is provided
        test_date = None
        if args.date_cursor is not None:
            test_date = get_fetch_date(args.date_cursor)
            logger.info(f"Testing alerts API with date: {test_date}")
        
        success = run_connectivity_tests(api_client, s3_uploader, test_date=test_date)
        sys.exit(0 if success else 1)
    
    clip_extractor = ClipExtractor(
        before_minutes=config["before_minutes"],
        after_minutes=config["after_minutes"],
        output_dir=config["output_dir"],
        chunk_duration_seconds=config["chunk_duration_seconds"],
        chunk_filename_pattern=config["chunk_filename_pattern"],
        local_source_dir=config["local_source_dir"]
    )
    
    # Initialize email sender if enabled
    email_sender = initialize_email_sender(config, logger)
    
    # Determine date to fetch alerts for
    fetch_date = get_fetch_date(args.date_cursor)
    if args.date_cursor is not None:
        days_ago = abs(args.date_cursor) if args.date_cursor < 0 else 0
        if args.date_cursor < 0:
            logger.info(f"Using date cursor {args.date_cursor} ({days_ago} day{'s' if days_ago != 1 else ''} ago): {fetch_date}")
        elif args.date_cursor > 0:
            logger.info(f"Using date cursor {args.date_cursor} ({args.date_cursor} day{'s' if args.date_cursor != 1 else ''} in future): {fetch_date}")
        else:
            logger.info(f"Using date cursor 0 (today): {fetch_date}")
        
        # Check status file - only process if status is EMPTY
        status, _, _ = read_status_file()
        if status and status != "EMPTY":
            logger.info(f"Status file shows '{status}', skipping processing (only process when status is EMPTY)")
            sys.exit(0)
    else:
        logger.info(f"No date cursor provided, using current date: {fetch_date}")
    
    # Fetch alerts
    try:
        with PerformanceLogger(logger, "fetch_alerts", fetch_date=fetch_date):
            alerts = api_client.get_alerts(fetch_date)
    except Exception as e:
        logger.error(f"Failed to fetch alerts: {e}", exc_info=True)
        sys.exit(1)
    
    if not alerts:
        logger.info(f"No alerts found for date {fetch_date}")
        write_status_file("EMPTY")
        sys.exit(0)
    
    # Determine status string based on date_cursor
    processing_status = "MF_PROCESSING" if args.date_cursor is not None else "PROCESSING"
    
    # Write PROCESSING/MF_PROCESSING status with total alerts count
    total_alerts = len(alerts)
    write_status_file(processing_status, total_count=total_alerts, processed_count=0)
    logger.info(f"Status file updated: {processing_status} with {total_alerts} total alerts")
    
    # Process each alert with progress bar
    successful = 0
    failed = 0
    processed_alerts = []
    
    with LoggingTqdm(total=len(alerts), desc="Processing alerts", unit="alert", 
                     bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                     resume_logger=resume_logger) as pbar:
        for alert in alerts:
            alert_id = alert.get("id")
            alert_logger = get_logger(__name__, {"correlation_id": correlation_id, "alert_id": alert_id})
            
            pbar.set_description(f"Processing alert {alert_id}")
            
            with PerformanceLogger(alert_logger, f"process_alert_{alert_id}", alert_id=alert_id):
                success, video_url, thumbnail_url = process_alert(
                    alert, clip_extractor, s3_uploader, api_client,
                    max_retries=config["max_retries"], retry_delay_seconds=config["retry_delay_seconds"]
                )
            
            if success:
                successful += 1
                processed_alerts.append((alert, video_url, thumbnail_url))
                pbar.set_postfix({"✓": successful, "✗": failed})
            else:
                failed += 1
                pbar.set_postfix({"✓": successful, "✗": failed})
                logger.error(f"Alert {alert_id} processing failed", extra={"alert_id": alert_id})
            
            # Update status file with successful count
            write_status_file(processing_status, total_count=total_alerts, processed_count=successful)
            
            pbar.update(1)
    
    # Send batch email with all processed alerts if email sender is configured
    if email_sender and processed_alerts:
        with LoggingTqdm(desc="Sending email notification", total=1, 
                         bar_format='{desc}: {elapsed}', resume_logger=resume_logger) as pbar:
            with PerformanceLogger(logger, "send_batch_email", alert_count=len(processed_alerts)):
                email_sender.send_batch_alert_email(processed_alerts)
            pbar.update(1)
    
    # Write FINISHED status
    write_status_file("FINISHED", total_count=total_alerts, processed_count=successful)
    logger.info(f"Status file updated: FINISHED with {total_alerts} total alerts, {successful} successfully processed")
    
    # Cleanup recordings for the processed date
    cleanup_recordings(fetch_date)
    
    # Final summary
    print(f"\n✓ Completed: {successful} | ✗ Failed: {failed} | Total: {len(alerts)}")
    
    if failed > 0:
        logger.warning(f"Exiting with error code due to {failed} failed alerts")
        sys.exit(1)


if __name__ == "__main__":
    main()
