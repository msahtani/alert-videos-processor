"""
Clip Extractor for S3 Video Chunks
Extracts video clips from S3-stored MP4 chunks for a given alert time
"""
import boto3
import datetime
import subprocess
import os
import logging
import re
from typing import Optional, List, Dict, Tuple
from pathlib import Path
from botocore.config import Config
from video_utils import ensure_browser_playable_mp4


class ClipExtractor:
    """Extracts video clips from S3-stored or local video chunks"""
    
    def __init__(self, region: str, s3_bucket: str, s3_prefix: str, 
                 before_minutes: int, after_minutes: int, output_dir: str,
                 chunk_duration_seconds: int = 300, chunk_filename_pattern: str = None,
                 local_source_dir: str = None):
        """
        Initialize clip extractor
        
        Args:
            region: AWS region
            s3_bucket: S3 bucket name containing video chunks
            s3_prefix: S3 key prefix for video chunks (e.g., alerts/)
            before_minutes: Minutes before alert time to include
            after_minutes: Minutes after alert time to include
            output_dir: Directory to save temporary clip files
            chunk_duration_seconds: Duration of each chunk in seconds (default: 300 = 5 minutes)
            chunk_filename_pattern: Regex pattern for chunk filenames (default: gcam_DDMMYYYY_HHMMSS.mp4)
            local_source_dir: Local directory containing video chunks (if provided, S3 is not used)
        """
        self.region = region
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip('/') + '/' if s3_prefix else ''
        self.before_minutes = before_minutes
        self.after_minutes = after_minutes
        self.output_dir = output_dir
        self.chunk_duration_seconds = chunk_duration_seconds
        self.local_source_dir = local_source_dir
        
        # Default filename pattern: gcam_DDMMYYYY_HHMMSS.mp4
        if chunk_filename_pattern is None:
            self.filename_re = re.compile(r"gcam_(\d{2})(\d{2})(\d{4})_(\d{2})(\d{2})(\d{2})\.mp4")
        else:
            self.filename_re = re.compile(chunk_filename_pattern)
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Only initialize S3 client if not using local source
        self.s3_client = None
        if not self.local_source_dir:
            # Configure boto3 with retries and timeouts for better reliability
            config = Config(
                retries={
                    'max_attempts': 3,
                    'mode': 'adaptive'
                },
                read_timeout=300,  # 5 minutes
                connect_timeout=60  # 1 minute
            )
            
            # Create boto3 S3 client (credentials from environment variables)
            self.client_kwargs = {"region_name": self.region, "config": config}
            self.s3_client = boto3.client("s3", **self.client_kwargs)
        else:
            logging.info(f"Using local source directory: {self.local_source_dir}")
    
    def _check_credentials(self):
        """Check if AWS credentials are available (skipped when using local source)"""
        # Skip credential check if using local source directory
        if self.local_source_dir:
            return True
            
        try:
            # Try to create a session to validate credentials
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
    
    def _parse_chunk_start_time(self, filename: str) -> Optional[datetime.datetime]:
        """
        Parse chunk start time from filename
        
        Args:
            filename: Chunk filename (e.g., gcam_22122025_075030.mp4)
            
        Returns:
            Datetime object representing chunk start time, or None if parsing fails
        """
        match = self.filename_re.match(filename)
        if not match:
            return None
        
        # Extract date components: DD, MM, YYYY, HH, MM, SS
        d, mo, y, h, mi, s = map(int, match.groups())
        return datetime.datetime(y, mo, d, h, mi, s)
    
    def _list_chunks(self) -> List[Dict]:
        """
        List all video chunks from local directory or S3
        
        Returns:
            List of chunk dictionaries with keys: key/path, name, S (start time), E (end time)
        """
        chunks = []
        
        # Use local source directory if configured
        if self.local_source_dir:
            return self._list_local_chunks()
        
        # Otherwise, list from S3
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    filename = os.path.basename(key)
                    
                    # Parse start time from filename
                    start_time = self._parse_chunk_start_time(filename)
                    if not start_time:
                        continue
                    
                    # Calculate end time (chunk duration after start time)
                    end_time = start_time + datetime.timedelta(seconds=self.chunk_duration_seconds)
                    
                    chunks.append({
                        "key": key,
                        "name": filename,
                        "S": start_time,
                        "E": end_time
                    })
            
            # Sort chunks by start time
            chunks.sort(key=lambda x: x["S"])
            logging.debug(f"Found {len(chunks)} video chunks in S3")
            return chunks
            
        except Exception as e:
            logging.error(f"Failed to list chunks from S3: {e}")
            logging.exception("Full traceback:")
            return []
    
    def _list_local_chunks(self) -> List[Dict]:
        """
        List all video chunks from local directory
        
        Returns:
            List of chunk dictionaries with keys: path, name, S (start time), E (end time)
        """
        chunks = []
        
        if not os.path.exists(self.local_source_dir):
            logging.error(f"Local source directory does not exist: {self.local_source_dir}")
            return []
        
        try:
            for filename in os.listdir(self.local_source_dir):
                if not filename.endswith('.mp4'):
                    continue
                
                # Parse start time from filename
                start_time = self._parse_chunk_start_time(filename)
                if not start_time:
                    continue
                
                # Calculate end time (chunk duration after start time)
                end_time = start_time + datetime.timedelta(seconds=self.chunk_duration_seconds)
                
                filepath = os.path.join(self.local_source_dir, filename)
                
                chunks.append({
                    "path": filepath,
                    "name": filename,
                    "S": start_time,
                    "E": end_time
                })
            
            # Sort chunks by start time
            chunks.sort(key=lambda x: x["S"])
            logging.debug(f"Found {len(chunks)} video chunks in local directory")
            return chunks
            
        except Exception as e:
            logging.error(f"Failed to list chunks from local directory: {e}")
            logging.exception("Full traceback:")
            return []
    
    def _chunk_intersects_window(self, chunk: Dict, window_start: datetime.datetime, 
                                  window_end: datetime.datetime) -> bool:
        """
        Check if a chunk intersects with the time window
        
        Args:
            chunk: Chunk dictionary with S and E keys
            window_start: Start of time window
            window_end: End of time window
            
        Returns:
            True if chunk intersects window, False otherwise
        """
        return not (chunk["E"] <= window_start or chunk["S"] >= window_end)
    
    def _cleanup_temp_files(self, temp_files: List[str]):
        """
        Clean up temporary files
        
        Args:
            temp_files: List of file paths to clean up
        """
        for file_path in temp_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logging.debug(f"Cleaned up temporary file: {file_path}")
            except Exception as e:
                logging.warning(f"Failed to remove temporary file {file_path}: {e}")
    
    def _generate_thumbnail(self, video_file: str, alert_time: datetime.datetime) -> Optional[str]:
        """
        Generate a thumbnail image from the video
        
        Args:
            video_file: Path to the video file
            alert_time: Alert datetime for naming
            
        Returns:
            Path to the thumbnail image, or None if generation failed
        """
        timestamp = alert_time.strftime('%Y%m%d_%H%M%S')
        thumbnail_file = os.path.join(self.output_dir, f"thumb_{timestamp}.jpg")
        
        logging.info(f"Generating thumbnail from video...")
        
        try:
            # Extract frame at 1 second (or 10% of video duration, whichever is smaller)
            # Use scale to create 1280x720 thumbnail (16:9 aspect ratio)
            subprocess.run([
                "ffmpeg", "-y",
                "-i", video_file,
                "-ss", "00:00:01",  # Seek to 1 second
                "-vframes", "1",  # Capture single frame
                "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",  # Scale and pad to exact size
                "-q:v", "2",  # High quality JPEG
                thumbnail_file
            ], check=True, capture_output=True, text=True, timeout=60)
            
            if os.path.exists(thumbnail_file) and os.path.getsize(thumbnail_file) > 0:
                logging.info(f"Thumbnail generated: {thumbnail_file}")
                return thumbnail_file
            else:
                logging.warning("Thumbnail file was not created or is empty")
                return None
                
        except subprocess.CalledProcessError as e:
            logging.error(f"FFmpeg thumbnail generation failed: {e.stderr}")
            logging.error(f"FFmpeg stdout: {e.stdout}")
            return None
        except subprocess.TimeoutExpired:
            logging.error("FFmpeg timeout during thumbnail generation")
            return None
        except Exception as e:
            logging.error(f"Unexpected error generating thumbnail: {e}")
            return None
    
    def extract_clip(self, alert_time_iso: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract a video clip for the given alert time from S3 chunks
        
        Args:
            alert_time_iso: Alert datetime in ISO format
            
        Returns:
            Tuple of (video_file_path, thumbnail_file_path), or (None, None) if extraction failed
        """
        # Check credentials before proceeding
        if not self._check_credentials():
            return None, None
        
        logging.info(f"Starting clip extraction for alert time: {alert_time_iso}")
        
        # Parse alert time (strip timezone info if present)
        try:
            alert_time = datetime.datetime.fromisoformat(alert_time_iso.replace('Z', ''))
            # Remove timezone info if present
            if alert_time.tzinfo is not None:
                alert_time = alert_time.replace(tzinfo=None)
        except ValueError:
            # Try parsing without timezone
            alert_time = datetime.datetime.fromisoformat(alert_time_iso)
            if alert_time.tzinfo is not None:
                alert_time = alert_time.replace(tzinfo=None)
        
        logging.debug(f"Parsed alert time: {alert_time}")
        
        # Calculate time window
        before_seconds = self.before_minutes * 60
        after_seconds = self.after_minutes * 60
        window_start = alert_time - datetime.timedelta(seconds=before_seconds)
        window_end = alert_time + datetime.timedelta(seconds=after_seconds)
        
        logging.info(f"Clip time window: {window_start} to {window_end} (before: {self.before_minutes}min, after: {self.after_minutes}min)")
        
        # List all chunks from S3
        all_chunks = self._list_chunks()
        if not all_chunks:
            logging.error("No chunks found in S3 or failed to list chunks")
            return None, None
        
        # Find chunks that intersect with the time window
        selected_chunks = [c for c in all_chunks if self._chunk_intersects_window(c, window_start, window_end)]
        
        if not selected_chunks:
            logging.warning(f"No chunks intersect with time window {window_start} → {window_end}")
            return None, None
        
        logging.info(f"Found {len(selected_chunks)} chunk(s) intersecting time window")
        
        # Process each selected chunk
        part_files = []
        temp_files_to_cleanup = []
        
        try:
            for idx, chunk in enumerate(selected_chunks):
                logging.info(f"Processing chunk {idx + 1}/{len(selected_chunks)}: {chunk['name']}")
                
                part_mp4 = os.path.join(self.output_dir, f"part_{idx}.mp4")
                temp_files_to_cleanup.append(part_mp4)
                
                # Determine source file path (local or download from S3)
                if self.local_source_dir:
                    # Use local file directly
                    local_mp4 = chunk["path"]
                    logging.debug(f"Using local file: {local_mp4}")
                else:
                    # Download chunk from S3
                    local_mp4 = os.path.join(self.output_dir, chunk["name"])
                    temp_files_to_cleanup.append(local_mp4)
                    
                    logging.debug(f"Downloading {chunk['key']} from S3...")
                    try:
                        self.s3_client.download_file(self.s3_bucket, chunk["key"], local_mp4)
                        logging.debug(f"Downloaded chunk to {local_mp4}")
                    except Exception as e:
                        logging.error(f"Failed to download chunk {chunk['key']}: {e}")
                        self._cleanup_temp_files(temp_files_to_cleanup)
                        return None, None
                
                # Calculate intersection of chunk time range with window
                chunk_start = max(chunk["S"], window_start)
                chunk_end = min(chunk["E"], window_end)
                
                # Calculate offset and duration within the chunk
                offset_seconds = (chunk_start - chunk["S"]).total_seconds()
                duration_seconds = (chunk_end - chunk_start).total_seconds()
                
                logging.debug(f"Extracting segment: offset={offset_seconds}s, duration={duration_seconds}s")
                
                # Extract the relevant segment from the chunk
                try:
                    subprocess.run([
                        "ffmpeg", "-y",
                        "-ss", str(offset_seconds),
                        "-i", local_mp4,
                        "-t", str(duration_seconds),
                        "-c", "copy",
                        part_mp4
                    ], check=True, capture_output=True, text=True, timeout=60)
                except subprocess.CalledProcessError as e:
                    logging.error(f"FFmpeg segment extraction failed: {e.stderr}")
                    logging.error(f"FFmpeg stdout: {e.stdout}")
                    self._cleanup_temp_files(temp_files_to_cleanup)
                    return None, None
                except subprocess.TimeoutExpired:
                    logging.error("FFmpeg timeout during segment extraction")
                    self._cleanup_temp_files(temp_files_to_cleanup)
                    return None, None
                
                part_files.append(part_mp4)
            
            # Concatenate all parts into final video
            if not part_files:
                logging.error("No parts to concatenate")
                self._cleanup_temp_files(temp_files_to_cleanup)
                return None, None
            
            # Create concat file for ffmpeg
            timestamp = alert_time.strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(self.output_dir, f"alert_clip_{timestamp}.mp4")
            concat_file = os.path.join(self.output_dir, f"concat_{timestamp}.txt")
            temp_files_to_cleanup.append(concat_file)
            
            # Write concat file
            with open(concat_file, 'w', encoding='utf-8') as f:
                for part_file in part_files:
                    # Use absolute path and escape single quotes for ffmpeg
                    abs_path = os.path.abspath(part_file).replace('\\', '/')
                    f.write(f"file '{abs_path}'\n")
            
            logging.info(f"Concatenating {len(part_files)} part(s) into final video...")
            
            # First concatenate parts (using copy for speed)
            temp_concat_file = output_file.replace('.mp4', '_temp.mp4')
            temp_files_to_cleanup.append(temp_concat_file)
            
            try:
                subprocess.run([
                    "ffmpeg", "-y",
                    "-f", "concat",
                    "-safe", "0",
                    "-i", concat_file,
                    "-c", "copy",  # Copy streams without re-encoding for speed
                    temp_concat_file
                ], check=True, capture_output=True, text=True, timeout=300)
            except subprocess.CalledProcessError as e:
                logging.error(f"FFmpeg concatenation failed: {e.stderr}")
                logging.error(f"FFmpeg stdout: {e.stdout}")
                self._cleanup_temp_files(temp_files_to_cleanup)
                return None, None
            except subprocess.TimeoutExpired:
                logging.error("FFmpeg timeout during concatenation")
                self._cleanup_temp_files(temp_files_to_cleanup)
                return None, None
            
            # Verify concatenated file was created
            if not os.path.exists(temp_concat_file) or os.path.getsize(temp_concat_file) == 0:
                logging.error("Concatenated file is empty or doesn't exist")
                self._cleanup_temp_files(temp_files_to_cleanup)
                return None, None
            
            # Optimize for browser playback using video_utils (add faststart flag)
            logging.info("Optimizing video for browser playback (adding faststart flag)...")
            try:
                ensure_browser_playable_mp4(temp_concat_file, quiet=True)
                # Move optimized file to final output location
                os.replace(temp_concat_file, output_file)
                logging.info("Video optimized successfully for browser playback")
            except Exception as e:
                logging.error(f"Video optimization failed: {e}")
                logging.exception("Full traceback:")
                # Fallback: use non-optimized concatenated file
                logging.warning("Using non-optimized concatenated file")
                if os.path.exists(temp_concat_file):
                    os.replace(temp_concat_file, output_file)
            
            # Verify final output file was created
            if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
                logging.error("Final output file is empty or doesn't exist")
                self._cleanup_temp_files(temp_files_to_cleanup)
                return None, None
            
            output_size = os.path.getsize(output_file)
            logging.info(f"MP4 file created: {output_size / 1024 / 1024:.2f} MB")
            
            # Generate thumbnail from video
            thumbnail_file = self._generate_thumbnail(output_file, alert_time)
            
            # Clean up temporary files (but keep the final output and thumbnail)
            if output_file in temp_files_to_cleanup:
                temp_files_to_cleanup.remove(output_file)
            if thumbnail_file and thumbnail_file in temp_files_to_cleanup:
                temp_files_to_cleanup.remove(thumbnail_file)
            self._cleanup_temp_files(temp_files_to_cleanup)
            
            return output_file, thumbnail_file
            
        except Exception as e:
            logging.error(f"Unexpected error during clip extraction: {e}")
            logging.exception("Full traceback:")
            self._cleanup_temp_files(temp_files_to_cleanup)
            return None, None
