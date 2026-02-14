"""
Alert processing logic
"""
import time
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from clip_extractor import ClipExtractor
from s3_uploader import S3Uploader
from api_client import APIClient
from logger_config import get_logger, PerformanceLogger


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

