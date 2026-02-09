"""
Main orchestrator script for processing alerts and extracting video clips
"""
import configparser
import argparse
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple

from api_client import APIClient
from clip_extractor import ClipExtractor
from s3_uploader import S3Uploader
from email_sender import EmailSender
from logger_config import setup_logging, get_logger, PerformanceLogger
from tqdm import tqdm
from pathlib import Path


def get_status_file_path():
    """Get the path to the status file in $HOME"""
    home_dir = os.path.expanduser("~")
    return Path(home_dir) / "alert-processor-status.txt"


def read_status_file():
    """Read status file and return status, total_count, processed_count"""
    status_file = get_status_file_path()
    if not status_file.exists():
        return None, None, None
    
    try:
        with open(status_file, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f.readlines()]
        
        status = lines[0] if len(lines) > 0 else None
        total_count = int(lines[1]) if len(lines) > 1 and lines[1].isdigit() else None
        processed_count = int(lines[2]) if len(lines) > 2 and lines[2].isdigit() else None
        
        return status, total_count, processed_count
    except Exception as e:
        # Use basic logging if logger not yet initialized
        try:
            logger = get_logger(__name__)
            logger.warning(f"Failed to read status file: {e}", exc_info=True)
        except:
            print(f"Warning: Failed to read status file: {e}")
        return None, None, None


def write_status_file(status, total_count=None, processed_count=None):
    """Write status file with status, total_count, and processed_count"""
    status_file = get_status_file_path()
    try:
        with open(status_file, 'w', encoding='utf-8') as f:
            f.write(f"{status}\n")
            if total_count is not None:
                f.write(f"{total_count}\n")
            if processed_count is not None:
                f.write(f"{processed_count}\n")
    except Exception as e:
        # Use basic logging if logger not yet initialized
        try:
            logger = get_logger(__name__)
            logger.error(f"Failed to write status file: {e}", exc_info=True)
        except:
            print(f"Error: Failed to write status file: {e}")


def cleanup_recordings(fetch_date):
    """Delete recordings matching pattern DDMMYYYY_*.mp4 (e.g., 08022026_221814.mp4)"""
    try:
        # Extract date from fetch_date (format: 2026-02-08T00:00:00Z)
        date_part = fetch_date.split('T')[0]  # Get YYYY-MM-DD part
        
        # Parse date and convert to DDMMYYYY format
        try:
            date_obj = datetime.strptime(date_part, '%Y-%m-%d')
            date_pattern = date_obj.strftime('%d%m%Y')  # Convert to DDMMYYYY (e.g., 08022026)
        except Exception as e:
            try:
                logger = get_logger(__name__)
                logger.error(f"Failed to parse date {date_part}: {e}", exc_info=True)
            except:
                pass
            return
        
        # Get recordings directory path
        recordings_dir = Path(os.path.expanduser("~")) / "recordings"
        
        if not recordings_dir.exists():
            try:
                logger = get_logger(__name__)
                logger.debug(f"Recordings directory does not exist: {recordings_dir}")
            except:
                pass
            return
        
        # Find all files matching pattern DDMMYYYY_*.mp4
        pattern = f"{date_pattern}_*.mp4"
        matching_files = list(recordings_dir.glob(pattern))
        
        if not matching_files:
            try:
                logger = get_logger(__name__)
                logger.debug(f"No recordings found matching pattern: {pattern}")
            except:
                pass
            return
        
        # Delete matching files
        deleted_count = 0
        logger = get_logger(__name__)
        for file_path in matching_files:
            try:
                file_path.unlink()
                deleted_count += 1
                logger.debug(f"Deleted recording file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to delete recording file {file_path}: {e}", exc_info=True)
        
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} recording file(s) matching pattern: {pattern}")
    except Exception as e:
        try:
            logger = get_logger(__name__)
            logger.error(f"Failed to cleanup recordings: {e}", exc_info=True)
        except:
            print(f"Error: Failed to cleanup recordings: {e}")


class LoggingTqdm(tqdm):
    """Custom tqdm that logs progress updates to resume log file"""
    
    def __init__(self, *args, resume_logger=None, **kwargs):
        self.resume_logger = resume_logger
        super().__init__(*args, **kwargs)
        if self.resume_logger:
            self.resume_logger.info(f"Started: {self.desc}")
    
    def update(self, n=1):
        result = super().update(n)
        if self.resume_logger and self.n > 0:
            # Log progress update - use str() representation which tqdm provides safely
            try:
                # Use tqdm's string representation which handles all attributes safely
                progress_str = str(self)
                if progress_str:
                    self.resume_logger.info(progress_str.strip())
            except Exception:
                # Fallback to simple logging if formatting fails
                try:
                    elapsed = getattr(self, 'elapsed', 0)
                    if elapsed == 0 and hasattr(self, 'start_t') and hasattr(self, '_time'):
                        elapsed = self._time() - self.start_t
                    elapsed_str = f"{int(elapsed//60):02d}:{int(elapsed%60):02d}" if elapsed > 0 else "00:00"
                    total_str = f"/{self.total}" if self.total else ""
                    self.resume_logger.info(f"{self.desc}: {self.n}{total_str} {self.unit} [{elapsed_str}]")
                except Exception:
                    # Ultimate fallback
                    self.resume_logger.info(f"{self.desc}: {self.n} {self.unit}")
        return result
    
    def set_description(self, desc=None, refresh=True):
        result = super().set_description(desc, refresh=refresh)
        if self.resume_logger and desc:
            self.resume_logger.info(f"Status: {desc}")
        return result
    
    def set_postfix(self, ordered_dict=None, refresh=True, **kwargs):
        result = super().set_postfix(ordered_dict, refresh=refresh, **kwargs)
        if self.resume_logger and (ordered_dict or kwargs):
            postfix_str = self.postfix if hasattr(self, 'postfix') else ""
            if postfix_str:
                self.resume_logger.info(f"Postfix: {postfix_str}")
        return result
    
    def close(self):
        if self.resume_logger:
            self.resume_logger.info(f"Completed: {self.desc}")
        return super().close()


def setup_aws_credentials(config):
    """Set up AWS credentials from environment variables"""
    import os
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
    import os
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
    
    # Get logger with alert context
    logger = get_logger(__name__, {"alert_id": alert_id})
    
    if not alert_id or not alert_date:
        logger.error(f"Alert missing required fields (id or alertDate): {alert}")
        return False, None, None
    
    logger.info(f"Processing alert with date {alert_date}", extra={"alert_date": alert_date})
    
    # Extract clip with retry logic for network failures
    mp4_file = None
    thumbnail_file = None
    retry_delay = retry_delay_seconds
    
    for attempt in range(max_retries):
        with PerformanceLogger(logger, "extract_clip", attempt=attempt + 1):
            mp4_file, thumbnail_file = clip_extractor.extract_clip(alert_date)
        
        if mp4_file:
            break
        
        if attempt < max_retries - 1:
            logger.warning(
                f"Clip extraction failed, retrying in {retry_delay} seconds...",
                extra={"attempt": attempt + 1, "max_retries": max_retries}
            )
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
        else:
            logger.error(
                f"Failed to extract clip after {max_retries} attempts",
                extra={"max_retries": max_retries}
            )
            return False, None, None
    
    if not mp4_file:
        logger.error("Failed to extract clip")
        return False, None, None
    
    # Generate timestamp for S3 key (from alert_date - must use alertDate, not current date)
    try:
        alert_time = datetime.fromisoformat(alert_date.replace('Z', '+00:00'))
        if alert_time.tzinfo is None:
            alert_time = alert_time.replace(tzinfo=timezone.utc)
        else:
            alert_time = alert_time.astimezone(timezone.utc)
        timestamp = alert_time.strftime('%Y%m%d_%H%M%S')
        logger.debug(f"Generated timestamp from alertDate", extra={"timestamp": timestamp})
    except Exception as e:
        logger.error(f"Failed to parse alert date: {e}", extra={"alert_date": alert_date}, exc_info=True)
        logger.error("Cannot generate clip name without valid alertDate. Skipping this alert.")
        return False, None, None
    
    # Upload video to S3
    with PerformanceLogger(logger, "upload_video_to_s3", timestamp=timestamp):
        s3_url = s3_uploader.upload_file(mp4_file, timestamp)
    
    if not s3_url:
        logger.error("Failed to upload clip to S3")
        # Clean up local files
        s3_uploader.cleanup_local_file(mp4_file)
        if thumbnail_file:
            s3_uploader.cleanup_local_file(thumbnail_file)
        return False, None, None
    
    # Upload thumbnail to S3 if available
    thumbnail_url = None
    if thumbnail_file:
        with PerformanceLogger(logger, "upload_thumbnail_to_s3", timestamp=timestamp):
            thumbnail_url = s3_uploader.upload_thumbnail(thumbnail_file, timestamp)
        
        if thumbnail_url:
            logger.info(f"Thumbnail uploaded", extra={"thumbnail_url": thumbnail_url})
        else:
            logger.warning("Failed to upload thumbnail, continuing without thumbnail")
    
    # Update API
    try:
        with PerformanceLogger(logger, "update_api_secondary_video"):
            api_client.update_secondary_video(alert_id, s3_url, thumbnail_url or "")
        
        logger.info(
            "Successfully processed alert",
            extra={"video_url": s3_url, "thumbnail_url": thumbnail_url}
        )
        
        # Clean up local files after successful upload and API update
        s3_uploader.cleanup_local_file(mp4_file)
        if thumbnail_file:
            s3_uploader.cleanup_local_file(thumbnail_file)
        return True, s3_url, thumbnail_url
    except Exception as e:
        logger.error(f"Failed to update API: {e}", exc_info=True)
        # Keep local file for debugging if API update fails
        return False, None, None


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
        "--no-wait",
        action="store_true",
        help="Skip waiting for outcome-comparator task completion and process alerts directly"
    )
    
    args = parser.parse_args()
    
    # Setup minimal logging (errors to file, suppress console output for progress bars)
    log_level = os.environ.get("LOG_LEVEL", "INFO")  # Log INFO+ to file
    log_dir = os.environ.get("LOG_DIR", "logs")
    json_logging = os.environ.get("JSON_LOGGING", "false").lower() == "true"
    
    setup_logging(
        log_level=log_level,
        log_dir=log_dir,
        log_file="alert_processor.log",
        json_logging=json_logging,
        verbose=args.verbose
    )
    
    # Setup resume log file for progress bar updates
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
    resume_logger.propagate = False  # Don't propagate to root logger
    
    # Get logger with correlation ID for this run (for file logging only)
    correlation_id = str(uuid.uuid4())
    logger = get_logger(__name__, {"correlation_id": correlation_id})
    
    # Load configuration first
    try:
        config = load_config(args.config)
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}", exc_info=True)
        sys.exit(1)
    
    # Set up AWS credentials from config file
    with PerformanceLogger(logger, "setup_aws_credentials"):
        setup_aws_credentials(config)
    
    # Check if using local source for loading videos
    local_source_dir = config.get("CLIP", "LOCAL_SOURCE_DIR", fallback=None)
    if local_source_dir:
        local_source_dir = local_source_dir.strip()
        if local_source_dir:
            local_source_dir = os.path.expandvars(local_source_dir)
            logger.info(f"Loading source videos from local directory: {local_source_dir}")
    
    # AWS credentials are always required for uploading processed clips to S3
    if not check_aws_credentials():
        logger.error("AWS credentials are required for uploading processed clips to S3")
        sys.exit(1)
    
    # Initialize components
    try:
        # AWS Configuration - region from environment variable
        aws_region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
        if not aws_region:
            logger.error("AWS region not found in environment variables!")
            logger.error("Please set AWS_DEFAULT_REGION or AWS_REGION environment variable")
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
            logger.error("STOREYES_BASE_URL environment variable not found!")
            logger.error("Please set STOREYES_BASE_URL environment variable")
            sys.exit(1)
        api_base_url = api_base_url.strip()
        alerts_endpoint = config.get("API", "ALERTS_ENDPOINT").strip()
        secondary_video_endpoint = config.get("API", "SECONDARY_VIDEO_ENDPOINT").strip()
        
        # Tasks API Configuration
        tasks_api_base_url = config.get("API", "TASKS_API_BASE_URL", fallback=None)
        if tasks_api_base_url:
            tasks_api_base_url = tasks_api_base_url.strip()
        tasks_endpoint = config.get("API", "TASKS_ENDPOINT", fallback="/api/tasks").strip()
        task_status_endpoint = config.get("API", "TASK_STATUS_ENDPOINT", fallback="/api/status/{task_id}").strip()
        store_code = config.get("API", "STORE_CODE", fallback=None)
        if store_code:
            store_code = store_code.strip()
        
        # Email Configuration (optional)
        email_enabled = config.getboolean("EMAIL", "ENABLED", fallback=False)
        
        logger.info("Configuration parsed successfully", extra={
            "s3_bucket": s3_bucket,
            "aws_region": aws_region,
            "email_enabled": email_enabled,
            "tasks_api_configured": tasks_api_base_url is not None,
            "store_code": store_code
        })
        
    except Exception as e:
        logger.error(f"Failed to read configuration: {e}", exc_info=True)
        sys.exit(1)
    
    # Initialize clients
    api_client = APIClient(
        base_url=api_base_url,
        alerts_endpoint=alerts_endpoint,
        secondary_video_endpoint=secondary_video_endpoint,
        tasks_api_base_url=tasks_api_base_url,
        tasks_endpoint=tasks_endpoint,
        task_status_endpoint=task_status_endpoint,
        store_code=store_code
    )
    
    # Check for outcome-comparator task completion before processing alerts
    if not args.no_wait:
        logger.info("Waiting for outcome-comparator task completion before processing alerts")
        with LoggingTqdm(desc="Waiting for outcome-comparator task", unit="check", 
                         bar_format='{desc}: {elapsed}', resume_logger=resume_logger) as pbar:
            while True:
                try:
                    tasks_data = api_client.get_tasks()
                    tasks = tasks_data.get("tasks", [])
                    
                    if not tasks:
                        pbar.set_description("Waiting for outcome-comparator task (no tasks found)")
                        for _ in range(30):  # 300 seconds = 30 * 10 second updates
                            time.sleep(10)
                            pbar.update(1)
                        continue
                    
                    # Find outcome-comparator task started within the last hour
                    outcome_comparator_task = None
                    current_time = datetime.now(timezone.utc)
                    
                    for task in tasks:
                        if task.get("type") == "outcome-comparator":
                            started_at_str = task.get("started_at")
                            if started_at_str:
                                try:
                                    started_at = datetime.fromisoformat(started_at_str.replace('Z', '+00:00'))
                                    if started_at.tzinfo is None:
                                        started_at = started_at.replace(tzinfo=timezone.utc)
                                    else:
                                        started_at = started_at.astimezone(timezone.utc)
                                    
                                    time_diff = current_time - started_at
                                    if time_diff.total_seconds() < 3600:
                                        outcome_comparator_task = task
                                        break
                                except Exception:
                                    pass
                            else:
                                outcome_comparator_task = task
                                break
                    
                    if outcome_comparator_task:
                        task_status = outcome_comparator_task.get("status")
                        task_id = outcome_comparator_task.get("task_id")
                        
                        if task_status == "completed":
                            pbar.set_description("Task completed, waiting 60s...")
                            for _ in range(6):  # 60 seconds = 6 * 10 second updates
                                time.sleep(10)
                                pbar.update(1)
                            break
                        else:
                            pbar.set_description(f"Task {task_id[:8]}... status: {task_status}")
                            for _ in range(30):  # 300 seconds
                                time.sleep(10)
                                pbar.update(1)
                            
                            try:
                                status_data = api_client.get_task_status(task_id)
                                status = status_data.get("status")
                                
                                if status == "completed":
                                    pbar.set_description("Task completed, waiting 60s...")
                                    for _ in range(6):
                                        time.sleep(10)
                                        pbar.update(1)
                                    break
                                else:
                                    continue
                            except Exception:
                                continue
                    else:
                        pbar.set_description("Waiting for outcome-comparator task (not found)")
                        for _ in range(30):
                            time.sleep(10)
                            pbar.update(1)
                        continue
                        
                except Exception as e:
                    logger.error(f"Failed to check tasks: {e}", exc_info=True)
                    pbar.set_description("Error checking tasks, retrying...")
                    for _ in range(30):
                        time.sleep(10)
                        pbar.update(1)
                    continue
    else:
        logger.info("Skipping wait for outcome-comparator task (--no-wait flag enabled)")
    
    # Log the workflow configuration
    if local_source_dir:
        logger.info(f"Source: Loading video chunks from local directory '{local_source_dir}'")
    else:
        logger.info(f"Source: Loading video chunks from S3 bucket '{s3_bucket}/{s3_prefix}'")
    logger.info(f"Destination: Uploading processed clips to S3 bucket '{s3_bucket}/{s3_upload_prefix}'")
    
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
            logger.info(f"Email notifications enabled. Sending to: {', '.join(to_emails)}")
        except Exception as e:
            logger.warning(f"Failed to initialize email sender: {e}", exc_info=True)
            logger.warning("Continuing without email notifications...")
            email_sender = None
    
    # Determine date to fetch alerts for
    if args.date_cursor is not None:
        # Calculate date based on cursor offset
        current_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        target_date = current_date + timedelta(days=args.date_cursor)
        fetch_date = target_date.strftime('%Y-%m-%dT%H:%M:%SZ')
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
        # Use current date at midnight UTC
        fetch_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
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
    
    # Write PROCESSING status with total alerts count
    total_alerts = len(alerts)
    write_status_file("PROCESSING", total_count=total_alerts, processed_count=0)
    logger.info(f"Status file updated: PROCESSING with {total_alerts} total alerts")
    
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
        logger.debug(f"Sorted {len(alerts)} alerts by alertDate")
    except Exception as e:
        logger.warning(f"Failed to sort alerts by alertDate: {e}. Processing in original order.", exc_info=True)
    
    # Process each alert with progress bar
    successful = 0
    failed = 0
    processed_alerts = []  # List of (alert, video_url, thumbnail_url) tuples for successful alerts
    
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
                    max_retries=max_retries, retry_delay_seconds=retry_delay_seconds
                )
            
            if success:
                successful += 1
                processed_alerts.append((alert, video_url, thumbnail_url))
                pbar.set_postfix({"✓": successful, "✗": failed})
            else:
                failed += 1
                pbar.set_postfix({"✓": successful, "✗": failed})
                logger.error(f"Alert {alert_id} processing failed", extra={"alert_id": alert_id})
            
            # Update status file with processed count
            processed_count = successful + failed
            write_status_file("PROCESSING", total_count=total_alerts, processed_count=processed_count)
            
            pbar.update(1)
    
    # Send batch email with all processed alerts if email sender is configured
    if email_sender and processed_alerts:
        with LoggingTqdm(desc="Sending email notification", total=1, 
                         bar_format='{desc}: {elapsed}', resume_logger=resume_logger) as pbar:
            with PerformanceLogger(logger, "send_batch_email", alert_count=len(processed_alerts)):
                email_sender.send_batch_alert_email(processed_alerts)
            pbar.update(1)
    
    # Write FINISHED status
    processed_count = successful + failed
    write_status_file("FINISHED", total_count=total_alerts, processed_count=processed_count)
    logger.info(f"Status file updated: FINISHED with {total_alerts} total alerts, {processed_count} processed")
    
    # Cleanup recordings for the processed date
    cleanup_recordings(fetch_date)
    
    # Final summary
    print(f"\n✓ Completed: {successful} | ✗ Failed: {failed} | Total: {len(alerts)}")
    
    if failed > 0:
        logger.warning(f"Exiting with error code due to {failed} failed alerts")
        sys.exit(1)


if __name__ == "__main__":
    main()

