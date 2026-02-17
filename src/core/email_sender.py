"""
Email sender module for sending alert notifications with video URLs
"""
import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Optional, List
from datetime import datetime


class EmailSender:
    """Handles sending email notifications for alerts"""
    
    def __init__(self, from_email: str, to_emails: List[str], use_tls: bool = True):
        """
        Initialize email sender
        
        Args:
            from_email: Email address to send from
            to_emails: List of recipient email addresses
            use_tls: Whether to use TLS encryption (default: True)
        """
        # Get SMTP settings from environment variables
        self.smtp_server = os.environ.get("SMTP_SERVER", "").strip()
        smtp_port_str = os.environ.get("SMTP_PORT", "587").strip()
        self.smtp_port = int(smtp_port_str) if smtp_port_str else 587
        self.smtp_username = os.environ.get("SMTP_USERNAME", "").strip()
        self.smtp_password = os.environ.get("SMTP_PASSWORD", "").strip()
        
        self.from_email = from_email
        self.to_emails = to_emails if isinstance(to_emails, list) else [to_emails]
        self.use_tls = use_tls
        
        logging.info(f"Email notifications enabled. Sending to: {', '.join(self.to_emails)}")
    
    def _create_email_body(self, alert: Dict, video_url: str) -> str:
        """
        Create HTML email body with alert details and video URL
        
        Args:
            alert: Alert dictionary with alert details
            video_url: URL of the video clip
            
        Returns:
            HTML formatted email body
        """
        alert_id = alert.get("id", "N/A")
        alert_date = alert.get("alertDate", "N/A")
        product_name = alert.get("productName", "N/A")
        status = alert.get("humanJudgement", "N/A")
        
        # Format the alert date for display
        try:
            if alert_date != "N/A":
                alert_datetime = datetime.fromisoformat(alert_date.replace('Z', '+00:00'))
                formatted_date = alert_datetime.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_date = alert_date
        except Exception:
            formatted_date = alert_date
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .header {{
                    background-color: #dc3545;
                    color: white;
                    padding: 15px;
                    border-radius: 5px 5px 0 0;
                    text-align: center;
                }}
                .content {{
                    background-color: #f8f9fa;
                    padding: 20px;
                    border: 1px solid #dee2e6;
                }}
                .alert-info {{
                    background-color: white;
                    padding: 15px;
                    margin: 10px 0;
                    border-left: 4px solid #dc3545;
                    border-radius: 4px;
                }}
                .info-row {{
                    margin: 8px 0;
                }}
                .info-label {{
                    font-weight: bold;
                    color: #495057;
                }}
                .video-link {{
                    display: inline-block;
                    background-color: #007bff;
                    color: white;
                    padding: 12px 24px;
                    text-decoration: none;
                    border-radius: 5px;
                    margin: 15px 0;
                    font-weight: bold;
                }}
                .video-link:hover {{
                    background-color: #0056b3;
                }}
                .footer {{
                    margin-top: 20px;
                    padding-top: 15px;
                    border-top: 1px solid #dee2e6;
                    font-size: 12px;
                    color: #6c757d;
                    text-align: center;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚨 Suspicious Event #{alert_id}</h2>
            </div>
            <div class="content">
                <div class="alert-info">
                    <div class="info-row">
                        <span class="info-label">Alert ID:</span> {alert_id}
                    </div>
                    <div class="info-row">
                        <span class="info-label">Timestamp:</span> {formatted_date}
                    </div>
                    <div class="info-row">
                        <span class="info-label">Product Type:</span> {product_name}
                    </div>
                    <div class="info-row">
                        <span class="info-label">Status:</span> {status}
                    </div>
                </div>
                
                <p>A video clip has been generated for this suspicious event. Click the link below to view the video:</p>
                
                <div style="text-align: center;">
                    <a href="{video_url}" class="video-link">📹 View Full Video</a>
                </div>
                
                <p style="margin-top: 20px; font-size: 12px; color: #6c757d;">
                    <strong>Direct Video URL:</strong><br>
                    <a href="{video_url}" style="word-break: break-all; color: #007bff;">{video_url}</a>
                </p>
            </div>
            <div class="footer">
                <p>This is an automated notification from the Kinesis Video ETL System.</p>
                <p>Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </body>
        </html>
        """
        return html_body
    
    def send_alert_email(self, alert: Dict, video_url: str) -> bool:
        """
        Send email notification for an alert with video URL
        
        Args:
            alert: Alert dictionary with alert details
            video_url: URL of the video clip
            
        Returns:
            True if email was sent successfully, False otherwise
        """
        alert_id = alert.get("id", "Unknown")
        
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Suspicious Event #{alert_id} - Video Available"
        msg['From'] = self.from_email
        msg['To'] = ', '.join(self.to_emails)
        
        # Create HTML body
        html_body = self._create_email_body(alert, video_url)
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        # Send email
        try:
            logging.info(f"Sending email notification for alert {alert_id} to {', '.join(self.to_emails)}")
            
            if self.use_tls:
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            
            server.login(self.smtp_username, self.smtp_password)
            server.send_message(msg)
            server.quit()
            
            logging.info(f"Successfully sent email notification for alert {alert_id}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to send email for alert {alert_id}: {e}")
            logging.exception("Full traceback:")
            return False
    
    def _create_batch_email_body(self, processed_alerts: List[tuple]) -> str:
        """
        Create HTML email body with multiple alerts and their video URLs
        
        Args:
            processed_alerts: List of (alert_dict, video_url, thumbnail_url) tuples
            
        Returns:
            HTML formatted email body
        """
        alert_count = len(processed_alerts)
        
        # Build alert entries HTML
        alert_entries_html = ""
        for alert_data in processed_alerts:
            # Handle both old format (alert, video_url) and new format (alert, video_url, thumbnail_url)
            if len(alert_data) == 3:
                alert, video_url, thumbnail_url = alert_data
            else:
                alert, video_url = alert_data
                thumbnail_url = None
            
            alert_id = alert.get("id", "N/A")
            alert_date = alert.get("alertDate", "N/A")
            product_name = alert.get("productName", "N/A")
            status = alert.get("humanJudgement", "N/A")
            
            # Format the alert date for display
            try:
                if alert_date != "N/A":
                    alert_datetime = datetime.fromisoformat(alert_date.replace('Z', '+00:00'))
                    formatted_date = alert_datetime.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_date = alert_date
            except Exception:
                formatted_date = alert_date
            
            # Build thumbnail HTML if available - make it clickable and well-styled
            thumbnail_html = ""
            if thumbnail_url:
                thumbnail_html = f"""
                    <div style="text-align: center; margin: 20px 0;">
                        <a href="{video_url}" style="display: inline-block; text-decoration: none;">
                            <img src="{thumbnail_url}" 
                                 alt="Video Thumbnail - Click to view video" 
                                 style="max-width: 100%; width: 640px; height: auto; 
                                        border-radius: 8px; border: 3px solid #007bff; 
                                        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                                        transition: transform 0.2s, box-shadow 0.2s;
                                        display: block; margin: 0 auto;"
                                 onmouseover="this.style.transform='scale(1.02)'; this.style.boxShadow='0 6px 12px rgba(0,0,0,0.15)';"
                                 onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='0 4px 8px rgba(0,0,0,0.1)';">
                        </a>
                        <p style="margin-top: 8px; font-size: 12px; color: #6c757d; font-style: italic;">
                            Click thumbnail to view video
                        </p>
                    </div>
                """
            
            alert_entries_html += f"""
                <div class="alert-info">
                    {thumbnail_html if thumbnail_url else ''}
                    <div class="info-row">
                        <span class="info-label">Alert ID:</span> {alert_id}
                    </div>
                    <div class="info-row">
                        <span class="info-label">Timestamp:</span> {formatted_date}
                    </div>
                    <div class="info-row">
                        <span class="info-label">Product Type:</span> {product_name}
                    </div>
                    <div class="info-row">
                        <span class="info-label">Status:</span> {status}
                    </div>
                    <div style="text-align: center; margin-top: 20px;">
                        <a href="{video_url}" class="video-link">📹 View Full Video</a>
                    </div>
                    <p style="margin-top: 15px; font-size: 11px; color: #6c757d; text-align: center;">
                        <a href="{video_url}" style="word-break: break-all; color: #007bff; text-decoration: none;">{video_url}</a>
                    </p>
                </div>
            """
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 700px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .header {{
                    background-color: #dc3545;
                    color: white;
                    padding: 15px;
                    border-radius: 5px 5px 0 0;
                    text-align: center;
                }}
                .content {{
                    background-color: #f8f9fa;
                    padding: 20px;
                    border: 1px solid #dee2e6;
                }}
                .summary {{
                    background-color: white;
                    padding: 15px;
                    margin-bottom: 20px;
                    border-radius: 4px;
                    text-align: center;
                    font-size: 18px;
                    font-weight: bold;
                    color: #dc3545;
                }}
                .alert-info {{
                    background-color: white;
                    padding: 15px;
                    margin: 15px 0;
                    border-left: 4px solid #dc3545;
                    border-radius: 4px;
                }}
                .info-row {{
                    margin: 8px 0;
                }}
                .info-label {{
                    font-weight: bold;
                    color: #495057;
                }}
                .video-link {{
                    display: inline-block;
                    background-color: #007bff;
                    color: white;
                    padding: 10px 20px;
                    text-decoration: none;
                    border-radius: 5px;
                    margin: 10px 0;
                    font-weight: bold;
                    font-size: 14px;
                }}
                .video-link:hover {{
                    background-color: #0056b3;
                }}
                .footer {{
                    margin-top: 20px;
                    padding-top: 15px;
                    border-top: 1px solid #dee2e6;
                    font-size: 12px;
                    color: #6c757d;
                    text-align: center;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚨 Suspicious Events Report</h2>
            </div>
            <div class="content">
                <div class="summary">
                    {alert_count} Alert{'s' if alert_count != 1 else ''} Processed
                </div>
                
                <p>The following suspicious events have been detected and processed. Video clips have been generated for each event:</p>
                
                {alert_entries_html}
            </div>
            <div class="footer">
                <p>This is an automated notification from the Kinesis Video ETL System.</p>
                <p>Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </body>
        </html>
        """
        return html_body
    
    def send_batch_alert_email(self, processed_alerts: List[tuple]) -> bool:
        """
        Send a single email notification with all processed alerts and their video URLs
        
        Args:
            processed_alerts: List of (alert_dict, video_url) tuples
            
        Returns:
            True if email was sent successfully, False otherwise
        """
        if not processed_alerts:
            logging.warning("No processed alerts to send in batch email")
            return False
        
        alert_count = len(processed_alerts)
        
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"{alert_count} Suspicious Event{'s' if alert_count != 1 else ''} - Video{'s' if alert_count != 1 else ''} Available"
        msg['From'] = self.from_email
        msg['To'] = ', '.join(self.to_emails)
        
        # Create HTML body
        html_body = self._create_batch_email_body(processed_alerts)
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        # Send email
        try:
            logging.info(f"Sending batch email notification for {alert_count} alert(s) to {', '.join(self.to_emails)}")
            
            if self.use_tls:
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            
            server.login(self.smtp_username, self.smtp_password)
            server.send_message(msg)
            server.quit()
            
            logging.info(f"Successfully sent batch email notification for {alert_count} alert(s)")
            return True
            
        except Exception as e:
            logging.error(f"Failed to send batch email: {e}")
            logging.exception("Full traceback:")
            return False



