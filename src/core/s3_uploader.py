"""
S3 Uploader for uploading video clips to S3
"""
import boto3
import os
import logging
from typing import Optional


class S3Uploader:
    """Handles uploading files to S3"""
    
    def __init__(self, region: str, bucket: str, prefix: str):
        """
        Initialize S3 uploader
        
        Args:
            region: AWS region
            bucket: S3 bucket name
            prefix: S3 key prefix (e.g., alerts/)
        """
        self.region = region
        self.bucket = bucket
        self.prefix = prefix.rstrip('/') + '/' if prefix else ''
        
        # Create boto3 S3 client (credentials from environment variables)
        self.client_kwargs = {"region_name": self.region}
        self.s3_client = boto3.client("s3", **self.client_kwargs)
    
    def _check_credentials(self):
        """Check if AWS credentials are available"""
        try:
            # Try to create a session to validate credentials
            import boto3
            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials is None:
                logging.error("AWS credentials not found!")
                logging.error("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
                return False
            return True
        except Exception as e:
            logging.error(f"Failed to validate AWS credentials: {e}")
            return False
    
    def upload_file(self, local_file_path: str, alert_timestamp: str) -> Optional[str]:
        """
        Upload a file to S3
        
        Args:
            local_file_path: Path to the local file to upload
            alert_timestamp: Timestamp string for generating S3 key (format: YYYYMMDD_HHMMSS)
            
        Returns:
            S3 URL of the uploaded file, or None if upload failed
        """
        # Check credentials before proceeding
        if not self._check_credentials():
            return None
        
        if not os.path.exists(local_file_path):
            logging.error(f"File does not exist: {local_file_path}")
            return None
        
        # Generate S3 key
        filename = os.path.basename(local_file_path)
        s3_key = f"{self.prefix}{alert_timestamp}.mp4"
        
        logging.info(f"Uploading {local_file_path} to s3://{self.bucket}/{s3_key}")
        
        try:
            # Upload with Content-Type header set to video/mp4 for browser compatibility
            # This is critical - browsers need this MIME type to play videos inline
            self.s3_client.upload_file(
                local_file_path, 
                self.bucket, 
                s3_key,
                ExtraArgs={'ContentType': 'video/mp4'}
            )
            
            # Generate S3 URL
            s3_url = f"s3://{self.bucket}/{s3_key}"
            
            # Also generate HTTPS URL
            s3_https_url = f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{s3_key}"
            
            logging.info(f"Successfully uploaded to {s3_url} with Content-Type: video/mp4")
            logging.debug(f"S3 HTTPS URL: {s3_https_url}")
            
            # Return HTTPS URL for API usage
            return s3_https_url
        except Exception as e:
            logging.error(f"Failed to upload file to S3: {e}")
            logging.exception("Full traceback:")
            return None
    
    def upload_thumbnail(self, local_file_path: str, alert_timestamp: str) -> Optional[str]:
        """
        Upload a thumbnail image to S3
        
        Args:
            local_file_path: Path to the local thumbnail file to upload
            alert_timestamp: Timestamp string for generating S3 key (format: YYYYMMDD_HHMMSS)
            
        Returns:
            HTTPS URL of the uploaded thumbnail, or None if upload failed
        """
        # Check credentials before proceeding
        if not self._check_credentials():
            return None
        
        if not os.path.exists(local_file_path):
            logging.error(f"Thumbnail file does not exist: {local_file_path}")
            return None
        
        # Generate S3 key for thumbnail (use alerts/thumbs/ prefix)
        s3_key = f"alerts/thumbs/{alert_timestamp}.jpg"
        
        logging.info(f"Uploading thumbnail {local_file_path} to s3://{self.bucket}/{s3_key}")
        
        try:
            # Upload thumbnail with correct Content-Type for images
            self.s3_client.upload_file(
                local_file_path, 
                self.bucket, 
                s3_key,
                ExtraArgs={'ContentType': 'image/jpeg'}
            )
            
            # Generate HTTPS URL
            s3_https_url = f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{s3_key}"
            
            logging.info(f"Successfully uploaded thumbnail to {s3_https_url} with Content-Type: image/jpeg")
            return s3_https_url
        except Exception as e:
            logging.error(f"Failed to upload thumbnail to S3: {e}")
            logging.exception("Full traceback:")
            return None
    
    def cleanup_local_file(self, local_file_path: str):
        """
        Remove local file after upload
        
        Args:
            local_file_path: Path to the local file to remove
        """
        try:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                logging.debug(f"Removed local file: {local_file_path}")
        except Exception as e:
            logging.warning(f"Failed to remove local file {local_file_path}: {e}")

