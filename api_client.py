"""
API Client for interacting with the alerts API
Handles fetching alerts and updating alert secondary video URLs
"""
import os
import requests
import logging
from typing import List, Dict, Optional
from datetime import datetime
from logger_config import get_logger, PerformanceLogger


class APIClient:
    """Client for interacting with the alerts API"""
    
    def __init__(self, base_url: str, alerts_endpoint: str, secondary_video_endpoint: str):
        """
        Initialize API client

        Args:
            base_url: Base URL of the API (e.g., http://49.13.89.74:8080)
            alerts_endpoint: Endpoint for fetching alerts (e.g., /api/alerts)
            secondary_video_endpoint: Endpoint template for updating secondary video (e.g., /api/alerts/{alert_id}/secondary-video)
        """
        self.base_url = base_url.rstrip('/')
        self.alerts_endpoint = alerts_endpoint
        self.secondary_video_endpoint = secondary_video_endpoint
        self.logger = get_logger(__name__)
    
    def get_alerts(self, date: str) -> List[Dict]:
        """
        Fetch alerts for a specific date
        
        Args:
            date: Date in ISO format (e.g., 2025-12-10T00:00:00)
        
        Returns:
            List of alert dictionaries

        Raises:
            requests.RequestException: If the API request fails
        """
        url = f"{self.base_url}{self.alerts_endpoint}"
        store_id = os.environ.get("STOREYES_STORE_ID", "")
        params = {"date": date, "store_id": store_id, "unprocessed": "true"}

        self.logger.info(f"Fetching alerts from {url}", extra={"date": date, "store_id": store_id})

        try:
            with PerformanceLogger(self.logger, "get_alerts", date=date):
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                alerts = response.json()
            
            self.logger.info(f"Retrieved alerts", extra={"alert_count": len(alerts)})
            return alerts
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch alerts: {e}", exc_info=True)
            raise

    def update_secondary_video(self, alert_id: int, secondary_video_url: str, image_url: str) -> bool:
        """
        Update the secondary video URL and image URL for an alert
        
        Args:
            alert_id: ID of the alert to update
            secondary_video_url: S3 URL of the secondary video
            image_url: S3 URL of the image
            
        Returns:
            True if update was successful, False otherwise
            
        Raises:
            requests.RequestException: If the API request fails
        """
        url = f"{self.base_url}{self.secondary_video_endpoint.format(alert_id=alert_id)}"
        payload = {
            "secondaryVideoUrl": secondary_video_url,
            "imageUrl": image_url
        }
        headers = {
            "Content-Type": "application/json"
        }
        
        self.logger.info(
            f"Updating alert with secondary video URL",
            extra={"alert_id": alert_id, "video_url": secondary_video_url, "image_url": image_url}
        )
        
        try:
            with PerformanceLogger(self.logger, "update_secondary_video", alert_id=alert_id):
                response = requests.put(url, json=payload, headers=headers, timeout=30)
                response.raise_for_status()
            
            self.logger.info(f"Successfully updated alert", extra={"alert_id": alert_id})
            return True
        except requests.RequestException as e:
            error_extra = {"alert_id": alert_id}
            if hasattr(e, 'response') and e.response is not None:
                error_extra["response_status"] = e.response.status_code
                error_extra["response_body"] = e.response.text
            
            self.logger.error(f"Failed to update alert: {e}", extra=error_extra, exc_info=True)
            raise
    
    def get_tasks(self) -> Dict:
        """
        Fetch all tasks from the API
        
        Returns:
            Dictionary with "tasks" key containing list of task dictionaries
            
        Raises:
            requests.RequestException: If the API request fails
        """
        url = "http://13.49.65.46:8080/api/tasks"
        
        self.logger.info(f"Fetching tasks from {url}")
        
        try:
            with PerformanceLogger(self.logger, "get_tasks"):
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                tasks_data = response.json()
            
            task_count = len(tasks_data.get("tasks", []))
            self.logger.debug(f"Retrieved tasks", extra={"task_count": task_count})
            return tasks_data
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch tasks: {e}", exc_info=True)
            raise
    
    def get_task_status(self, task_id: str) -> Dict:
        """
        Get the status of a specific task
        
        Args:
            task_id: ID of the task to check
            
        Returns:
            Dictionary with task status information including "status" field
            
        Raises:
            requests.RequestException: If the API request fails
        """
        url = f"http://13.49.65.46:8080/api/status/{task_id}"
        
        self.logger.info(f"Fetching task status from {url}", extra={"task_id": task_id})
        
        try:
            with PerformanceLogger(self.logger, "get_task_status", task_id=task_id):
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                status_data = response.json()
            
            status = status_data.get("status", "unknown")
            self.logger.debug(f"Retrieved task status", extra={"task_id": task_id, "status": status})
            return status_data
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch task status: {e}", extra={"task_id": task_id}, exc_info=True)
            raise