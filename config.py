"""
Configuration management for NSE OI Spurts Bot
"""

import os
from datetime import time

class Config:
    """Application configuration"""
    
    # Bot Configuration
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8095839692:AAGtMCXOuAcDURa-VmDDFrq3UXqzfBbaH1g")
    
    # NSE Configuration
    NSE_OI_SPURTS_URL = "https://www.nseindia.com/market-data/oi-spurts"
    NSE_BASE_URL = "https://www.nseindia.com"
    
    # Scheduling Configuration
    MONITORING_START_TIME = time(10, 0)  # 10:00 AM
    MONITORING_END_TIME = time(14, 30)   # 2:30 PM
    SCRAPING_INTERVAL_MINUTES = 20
    
    # File Configuration
    DATA_DIR = "data"
    EXCEL_DIR = os.path.join(DATA_DIR, "excel_files")
    PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
    
    # Request Configuration
    REQUEST_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    
    # User Agent for web requests
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    
    # Data Configuration
    MAX_DAILY_FILES = 50  # Maximum Excel files to keep per day
    
    def __init__(self):
        """Initialize configuration and create necessary directories"""
        self._create_directories()
    
    def _create_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            self.DATA_DIR,
            self.EXCEL_DIR,
            self.PROCESSED_DATA_DIR
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
