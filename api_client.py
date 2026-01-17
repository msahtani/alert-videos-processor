"""
API Client for interacting with the alerts API
Handles fetching alerts and updating alert secondary video URLs
"""
import os
import requests
import logging
from typing import List, Dict, Optional
from datetime import datetime


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

        logging.info(f"Fetching alerts from {url} with date={date}")

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            alerts = response.json()
            logging.info(f"Retrieved {len(alerts)} alerts")
            return alerts
        except requests.RequestException as e:
            logging.error(f"Failed to fetch alerts: {e}")
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
        
        logging.info(f"Updating alert {alert_id} with secondary video URL: {secondary_video_url}, image URL: {image_url}")
        
        try:
            response = requests.put(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            logging.info(f"Successfully updated alert {alert_id}")
            return True
        except requests.RequestException as e:
            logging.error(f"Failed to update alert {alert_id}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Response status: {e.response.status_code}")
                logging.error(f"Response body: {e.response.text}")
            raise
