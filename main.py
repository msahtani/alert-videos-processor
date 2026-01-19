"""
Main orchestrator script for processing alerts and extracting video clips
"""
import configparser
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple

from api_client import APIClient
from clip_extractor import ClipExtractor
from s3_uploader import S3Uploader
from email_sender import EmailSender


def setup_logging(verbose=False):
    """Configure logging based on verbose flag"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout
    )


def setup_aws_credentials(config):
    """Set up AWS credentials from environment variables"""
    import os
    
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
        logging.debug("AWS credentials loaded from environment variables")
    
    if region:
        os.environ["AWS_DEFAULT_REGION"] = region


def check_aws_credentials():
    """Check if AWS credentials are available"""
    import os
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    
    if not access_key or not secret_key:
        logging.error("AWS credentials not found!")
        logging.error("Please add credentials to config.conf:")
        logging.error("  [AWS]")
        logging.error("  ACCESS_KEY_ID = your-access-key-id")
        logging.error("  SECRET_ACCESS_KEY = your-secret-access-key")
        return False
    
    logging.debug("AWS credentials configured")
    return True


def load_config(config_file="config.conf"):
    """Load configuration from config file"""
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def process_alert(alert: Dict, clip_extractor: ClipExtractor, 
                  s3_uploader: S3Uploader, api_client: APIClient, 
                  max_retries: int = 3, retry_delay_seconds: int = 2) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Process a single alert: extract clip, upload to S3, update API
    
    Args:
        alert: Alert dictionary with alertDate and id
        clip_extractor: ClipExtractor instance
        s3_uploader: S3Uploader instance
        api_client: APIClient instance
        max_retries: Maximum number of retry attempts for network failures
        retry_delay_seconds: Initial delay between retries (doubles with each retry)
        
    Returns:
        Tuple of (success: bool, video_url: Optional[str], thumbnail_url: Optional[str])
        Returns (True, video_url, thumbnail_url) if successful, (False, None, None) if failed
    """
    alert_id = alert.get("id")
    alert_date = alert.get("alertDate")
    
    if not alert_id or not alert_date:
        logging.error(f"Alert missing required fields (id or alertDate): {alert}")
        return False, None
    
    logging.info(f"Processing alert {alert_id} with date {alert_date}")
    
    # Extract clip with retry logic for network failures
    mp4_file = None
    thumbnail_file = None
    retry_delay = retry_delay_seconds
    
    for attempt in range(max_retries):
        mp4_file, thumbnail_file = clip_extractor.extract_clip(alert_date)
        if mp4_file:
            break
        
        if attempt < max_retries - 1:
            logging.warning(f"Clip extraction failed (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
        else:
            logging.error(f"Failed to extract clip for alert {alert_id} after {max_retries} attempts")
            return False, None, None
    
    if not mp4_file:
        logging.error(f"Failed to extract clip for alert {alert_id}")
        return False, None, None
    
    # Generate timestamp for S3 key (from alert_date - must use alertDate, not current date)
    try:
        alert_time = datetime.fromisoformat(alert_date.replace('Z', '+00:00'))
        if alert_time.tzinfo is None:
            alert_time = alert_time.replace(tzinfo=timezone.utc)
        else:
            alert_time = alert_time.astimezone(timezone.utc)
        timestamp = alert_time.strftime('%Y%m%d_%H%M%S')
        logging.debug(f"Generated timestamp from alertDate {alert_date}: {timestamp}")
    except Exception as e:
        logging.error(f"Failed to parse alert date {alert_date}: {e}")
        logging.error("Cannot generate clip name without valid alertDate. Skipping this alert.")
        return False, None, None
    
    # Upload video to S3
    s3_url = s3_uploader.upload_file(mp4_file, timestamp)
    if not s3_url:
        logging.error(f"Failed to upload clip to S3 for alert {alert_id}")
        # Clean up local files
        s3_uploader.cleanup_local_file(mp4_file)
        if thumbnail_file:
            s3_uploader.cleanup_local_file(thumbnail_file)
        return False, None, None
    
    # Upload thumbnail to S3 if available
    thumbnail_url = None
    if thumbnail_file:
        thumbnail_url = s3_uploader.upload_thumbnail(thumbnail_file, timestamp)
        if thumbnail_url:
            logging.info(f"Thumbnail uploaded: {thumbnail_url}")
        else:
            logging.warning(f"Failed to upload thumbnail for alert {alert_id}, continuing without thumbnail")
    
    # Update API
    try:
        api_client.update_secondary_video(alert_id, s3_url, thumbnail_url or "")
        logging.info(f"Successfully processed alert {alert_id}")
        
        # Clean up local files after successful upload and API update
        s3_uploader.cleanup_local_file(mp4_file)
        if thumbnail_file:
            s3_uploader.cleanup_local_file(thumbnail_file)
        return True, s3_url, thumbnail_url
    except Exception as e:
        logging.error(f"Failed to update API for alert {alert_id}: {e}")
        # Keep local file for debugging if API update fails
        return False, None, None


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Process alerts and extract video clips")
    parser.add_argument(
        "--date",
        type=str,
        help="Date in ISO format to fetch alerts (e.g., 2025-12-10T00:00:00). If not provided, uses current date."
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
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose)
    
    # Load configuration first
    try:
        config = load_config(args.config)
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        sys.exit(1)
    
    # Set up AWS credentials from config file
    setup_aws_credentials(config)
    
    # Check if using local source for loading videos
    local_source_dir = config.get("CLIP", "LOCAL_SOURCE_DIR", fallback=None)
    if local_source_dir:
        local_source_dir = local_source_dir.strip()
        if local_source_dir:
            local_source_dir = os.path.expandvars(local_source_dir)
            logging.info(f"Loading source videos from local directory: {local_source_dir}")
    
    # AWS credentials are always required for uploading processed clips to S3
    if not check_aws_credentials():
        logging.error("AWS credentials are required for uploading processed clips to S3")
        sys.exit(1)
    
    # Initialize components
    try:
        # AWS Configuration - region from environment variable
        import os
        aws_region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
        if not aws_region:
            logging.error("AWS region not found in environment variables!")
            logging.error("Please set AWS_DEFAULT_REGION or AWS_REGION environment variable")
            sys.exit(1)
        aws_region = aws_region.strip()
        s3_bucket = config.get("AWS", "S3_BUCKET").strip()
        s3_prefix = config.get("AWS", "S3_PREFIX").strip()
        s3_upload_prefix = config.get("AWS", "S3_UPLOAD_PREFIX", fallback="alerts/").strip()
        
        # Clip Configuration
        before_minutes = int(config.get("CLIP", "BEFORE_MINUTES").strip())
        after_minutes = int(config.get("CLIP", "AFTER_MINUTES").strip())
        output_dir = config.get("CLIP", "OUTPUT_DIR").strip()
        chunk_duration_seconds = int(config.get("CLIP", "CHUNK_DURATION_SECONDS", fallback="300").strip())
        chunk_filename_pattern = config.get("CLIP", "CHUNK_FILENAME_PATTERN", fallback=None)
        if chunk_filename_pattern:
            chunk_filename_pattern = chunk_filename_pattern.strip()
        local_source_dir = config.get("CLIP", "LOCAL_SOURCE_DIR", fallback=None)
        if local_source_dir:
            local_source_dir = local_source_dir.strip()
            if local_source_dir:  # Only expand if not empty
                local_source_dir = os.path.expandvars(local_source_dir)
            else:  # Empty string means use S3
                local_source_dir = None
        
        # Processing Configuration
        max_retries = int(config.get("PROCESSING", "MAX_RETRIES", fallback="3").strip())
        retry_delay_seconds = int(config.get("PROCESSING", "RETRY_DELAY_SECONDS", fallback="2").strip())
        
        # API Configuration - base URL from environment variable
        api_base_url = os.environ.get("STOREYES_BASE_URL")
        if not api_base_url:
            logging.error("STOREYES_BASE_URL environment variable not found!")
            logging.error("Please set STOREYES_BASE_URL environment variable")
            sys.exit(1)
        api_base_url = api_base_url.strip()
        alerts_endpoint = config.get("API", "ALERTS_ENDPOINT").strip()
        secondary_video_endpoint = config.get("API", "SECONDARY_VIDEO_ENDPOINT").strip()
        
        # Email Configuration (optional)
        email_enabled = config.getboolean("EMAIL", "ENABLED", fallback=False)
        
    except Exception as e:
        logging.error(f"Failed to read configuration: {e}")
        sys.exit(1)
    
    # Initialize clients
    api_client = APIClient(api_base_url, alerts_endpoint, secondary_video_endpoint)
    
    # Log the workflow configuration
    if local_source_dir:
        logging.info(f"Source: Loading video chunks from local directory '{local_source_dir}'")
    else:
        logging.info(f"Source: Loading video chunks from S3 bucket '{s3_bucket}/{s3_prefix}'")
    logging.info(f"Destination: Uploading processed clips to S3 bucket '{s3_bucket}/{s3_upload_prefix}'")
    
    clip_extractor = ClipExtractor(
        region=aws_region,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        before_minutes=before_minutes,
        after_minutes=after_minutes,
        output_dir=output_dir,
        chunk_duration_seconds=chunk_duration_seconds,
        chunk_filename_pattern=chunk_filename_pattern,
        local_source_dir=local_source_dir
    )
    s3_uploader = S3Uploader(aws_region, s3_bucket, s3_upload_prefix)
    
    # Initialize email sender if enabled
    email_sender = None
    if email_enabled:
        try:
            import os
            smtp_server = os.environ.get("SMTP_SERVER", "").strip()
            smtp_port = int(os.environ.get("SMTP_PORT", "587").strip())
            smtp_username = os.environ.get("SMTP_USERNAME", "").strip()
            smtp_password = os.environ.get("SMTP_PASSWORD", "").strip()
            from_email = config.get("EMAIL", "FROM_EMAIL").strip()
            to_emails_str = config.get("EMAIL", "TO_EMAILS").strip()
            to_emails = [email.strip() for email in to_emails_str.split(',')]
            use_tls = config.getboolean("EMAIL", "USE_TLS", fallback=True)
            
            email_sender = EmailSender(
                smtp_server=smtp_server,
                smtp_port=smtp_port,
                smtp_username=smtp_username,
                smtp_password=smtp_password,
                from_email=from_email,
                to_emails=to_emails,
                use_tls=use_tls
            )
            logging.info(f"Email notifications enabled. Sending to: {', '.join(to_emails)}")
        except Exception as e:
            logging.warning(f"Failed to initialize email sender: {e}")
            logging.warning("Continuing without email notifications...")
            email_sender = None
    
    # Determine date to fetch alerts for
    if args.date:
        # Parse the provided date - user provides date in UTC+1 timezone
        try:
            # Try to parse the date string
            if 'T' in args.date:
                # ISO format with time
                provided_date = datetime.fromisoformat(args.date.replace('Z', '+00:00'))
            else:
                # Date only, assume midnight UTC+1
                provided_date = datetime.fromisoformat(args.date + 'T00:00:00+01:00')
            
            # Ensure timezone-aware
            if provided_date.tzinfo is None:
                # If no timezone info, assume UTC+1 (user's local timezone)
                utc_plus_one = timezone(timedelta(hours=1))
                provided_date = provided_date.replace(tzinfo=utc_plus_one)
            
            # Convert UTC+1 to UTC (producer timestamp timezone)
            provided_date_utc = provided_date.astimezone(timezone.utc)
            
            # Format as ISO string with 'Z' to indicate UTC for API
            fetch_date = provided_date_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
            logging.info(f"Parsed date {args.date} (UTC+1) as UTC: {fetch_date}")
        except Exception as e:
            logging.error(f"Failed to parse date '{args.date}': {e}")
            logging.error("Please provide date in ISO format (e.g., 2025-12-10T12:00:00)")
            sys.exit(1)
    else:
        # Use current date at midnight UTC
        fetch_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        logging.info(f"No date provided, using current date: {fetch_date}")
    
    # Fetch alerts
    try:
        alerts = api_client.get_alerts(fetch_date)
    except Exception as e:
        logging.error(f"Failed to fetch alerts: {e}")
        sys.exit(1)
    
    if not alerts:
        logging.info(f"No alerts found for date {fetch_date}")
        sys.exit(0)
    
    # Sort alerts by alertDate (oldest first)
    def get_alert_datetime(alert):
        """Extract datetime from alert for sorting"""
        alert_date = alert.get("alertDate", "")
        if not alert_date:
            return datetime.min  # Put alerts without date at the beginning
        
        try:
            # Try parsing with timezone
            dt = datetime.fromisoformat(alert_date.replace('Z', '+00:00'))
            # Convert to UTC if timezone-aware, then make naive for comparison
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return datetime.min  # Put unparseable dates at the beginning
    
    try:
        alerts.sort(key=get_alert_datetime)
        logging.debug(f"Sorted {len(alerts)} alerts by alertDate")
    except Exception as e:
        logging.warning(f"Failed to sort alerts by alertDate: {e}. Processing in original order.")
    
    logging.info(f"Found {len(alerts)} alerts to process")
    
    # Process each alert
    successful = 0
    failed = 0
    processed_alerts = []  # List of (alert, video_url, thumbnail_url) tuples for successful alerts
    
    for alert in alerts:
        success, video_url, thumbnail_url = process_alert(
            alert, clip_extractor, s3_uploader, api_client,
            max_retries=max_retries, retry_delay_seconds=retry_delay_seconds
        )
        if success:
            successful += 1
            processed_alerts.append((alert, video_url, thumbnail_url))
        else:
            failed += 1
    
    logging.info(f"Processing complete: {successful} successful, {failed} failed")
    
    # Send batch email with all processed alerts if email sender is configured
    if email_sender and processed_alerts:
        logging.info(f"Sending batch email notification for {len(processed_alerts)} alert(s)")
        email_sender.send_batch_alert_email(processed_alerts)
    
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

