"""
Device utilities for retrieving device information
"""
import subprocess
from logger_config import get_logger


def get_device_id() -> str:
    """
    Get device ID from /proc/cpuinfo using awk command
    
    Returns:
        Device ID (serial number) as string
        
    Raises:
        RuntimeError: If device ID cannot be retrieved
    """
    try:
        result = subprocess.run(
            ["awk", "/Serial/ {print $3}", "/proc/cpuinfo"],
            capture_output=True,
            text=True,
            check=True
        )
        device_id = result.stdout.strip()
        if not device_id:
            raise RuntimeError("Device ID not found in /proc/cpuinfo")
        return device_id
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger = get_logger(__name__)
        logger.error(f"Failed to get device ID: {e}", exc_info=True)
        raise RuntimeError(f"Failed to get device ID from /proc/cpuinfo: {e}")

