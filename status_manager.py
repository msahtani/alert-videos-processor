"""
Status file management for tracking processing state
"""
import os
from pathlib import Path
from logger_config import get_logger


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

