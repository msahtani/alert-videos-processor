"""
Status file management for tracking processing state and MQTT publishing
"""
import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import paho.mqtt.client as mqtt
from src.utils.logger_config import get_logger


def get_status_file_path():
    """Get the path to the status file beside main.py"""
    # Get the directory where main.py is located (project root)
    # This assumes status_manager is in src/utils, so we go up 2 levels
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent
    return project_root / "alert-processor-status.txt"


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


def _publish_mqtt_status(board_id: str, status: str, total_count: Optional[int] = None, 
                         processed_count: Optional[int] = None) -> bool:
    """
    Publish status to MQTT broker
    
    Args:
        board_id: Device/board ID
        status: Status string (EMPTY, PROCESSING, MF_PROCESSING, FINISHED)
        total_count: Total number of alerts (optional)
        processed_count: Number of processed alerts (optional)
        
    Returns:
        True if published successfully, False otherwise
    """
    logger = get_logger(__name__)
    
    # MQTT configuration (same as alert-monitor.sh)
    mqtt_host = os.environ.get("MQTT_HOST", "18.100.207.236")
    mqtt_port = os.environ.get("MQTT_PORT", "1883")
    mqtt_user = os.environ.get("MQTT_USER", "storeyes")
    mqtt_pass = os.environ.get("MQTT_PASS", "12345")
    mqtt_topic = os.environ.get("MQTT_TOPIC", f"storeyes/{board_id}/alert-processor")
    qos = os.environ.get("QOS", "1")
    retain = os.environ.get("RETAIN", "false").lower() == "true"
    timeout = int(os.environ.get("TIMEOUT", "5"))
    retries = int(os.environ.get("RETRIES", "3"))
    
    # Build JSON payload (same format as alert-monitor.sh)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "board_id": board_id,
        "timestamp": timestamp,
        "alert-processor": {
            "status": status,
            "total": str(total_count) if total_count is not None else "-1",
            "processed": str(processed_count) if processed_count is not None else "-1"
        }
    }
    
    # Convert to compact JSON
    try:
        json_payload = json.dumps(payload, separators=(',', ':'))
    except Exception as e:
        logger.error(f"Failed to serialize MQTT payload: {e}", exc_info=True)
        return False
    
    # Convert QOS to int
    qos_int = int(qos)
    
    # Retry loop (same as alert-monitor.sh)
    client = None
    for attempt in range(1, retries + 1):
        try:
            # Create MQTT client (use latest callback API version to avoid deprecation warning)
            client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            client.username_pw_set(mqtt_user, mqtt_pass)
            
            # Connect to broker
            client.connect(mqtt_host, int(mqtt_port), keepalive=60)
            
            # Start network loop to handle connection
            client.loop_start()
            
            # Wait a bit for connection to establish
            time.sleep(0.1)
            
            # Publish message
            result = client.publish(
                mqtt_topic,
                json_payload,
                qos=qos_int,
                retain=retain
            )
            
            # Wait for message to be published (with timeout)
            if result.wait_for_publish(timeout=timeout):
                # Stop network loop
                client.loop_stop()
                client.disconnect()
                logger.debug(f"MQTT status published successfully (attempt {attempt}/{retries})")
                return True
            else:
                logger.warning(f"MQTT publish timeout (attempt {attempt}/{retries})")
                client.loop_stop()
                client.disconnect()
                
        except Exception as e:
            logger.warning(f"MQTT publish error (attempt {attempt}/{retries}): {e}")
            if client:
                try:
                    client.loop_stop()
                    client.disconnect()
                except:
                    pass
        
        if attempt < retries:
            time.sleep(2)
    
    logger.error(f"Failed to publish MQTT status after {retries} attempts")
    return False


def write_status_file(status, total_count=None, processed_count=None, board_id: Optional[str] = None):
    """
    Write status file and publish to MQTT
    
    Args:
        status: Status string (EMPTY, PROCESSING, MF_PROCESSING, FINISHED)
        total_count: Total number of alerts (optional)
        processed_count: Number of processed alerts (optional)
        board_id: Device/board ID for MQTT (optional, will try to get from device_utils if not provided)
    """
    logger = get_logger(__name__)
    
    # Write to file (for backward compatibility and read_status_file)
    status_file = get_status_file_path()
    try:
        with open(status_file, 'w', encoding='utf-8') as f:
            f.write(f"{status}\n")
            if total_count is not None:
                f.write(f"{total_count}\n")
            if processed_count is not None:
                f.write(f"{processed_count}\n")
    except Exception as e:
        logger.error(f"Failed to write status file: {e}", exc_info=True)
    
    # Publish to MQTT
    try:
        if board_id is None:
            # Try to get board_id from device_utils
            try:
                from src.utils.device_utils import get_device_id
                board_id = get_device_id()
            except Exception as e:
                logger.debug(f"Could not get device ID for MQTT: {e}")
                return  # Skip MQTT if we can't get board_id
        
        _publish_mqtt_status(board_id, status, total_count, processed_count)
    except Exception as e:
        logger.warning(f"Failed to publish MQTT status: {e}", exc_info=True)
        # Don't fail the whole operation if MQTT fails

