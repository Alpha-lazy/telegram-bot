"""
NSE Oi Spurts Web Scraper
Handles downloading and parsing of NSE India Oi spurts data
"""

import asyncio
import logging
import os
import requests
from datetime import datetime, date
import time
import json
from typing import Dict, List, Optional, Tuple
import re
from urllib.parse import urljoin

from config import Config

logger = logging.getLogger(__name__)

class NSEScraper:
    """Handles scraping of NSE OI Spurts data"""
    
    def __init__(self):
        self.config = Config()
        self.session = requests.Session()
        self.setup_session()
        self.last_scrape_time = None
        
    def setup_session(self):
        """Setup requests session with proper headers"""
        self.session.headers.update({
            'User-Agent': self.config.USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
    def scrape_oi_spurts_data(self) -> Optional[Dict]:
        """
        Scrape Oi spurts data from NSE website
        Returns dict with scraped data or None if failed
        """
        try:
            logger.info("🔍 Starting OI spurts data scraping...")
            
            # First, get the main page to establish session
            main_response = self._make_request(self.config.NSE_OI_SPURTS_URL)
            if not main_response:
                return None
            
            # Look for the download link or API endpoint
            download_url = self._find_download_url(main_response.text)
            if not download_url:
                logger.error("❌ Could not find download URL for OI spurts data")
                return None
            
            # Download the Excel file
            excel_data = self._download_excel_file(download_url)
            if not excel_data:
                return None
            
            # Save the Excel file
            file_path = self._save_excel_file(excel_data)
            if not file_path:
                return None
            
            self.last_scrape_time = datetime.now()
            
            return {
                'success': True,
                'file_path': file_path,
                'timestamp': self.last_scrape_time.isoformat(),
                'size': len(excel_data),
                'url': download_url
            }
            
        except Exception as e:
            logger.error(f"❌ Error scraping OI spurts data: {e}")
            return None
    
    def _make_request(self, url: str, retries: int = None) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        if retries is None:
            retries = self.config.MAX_RETRIES
            
        for attempt in range(retries + 1):
            try:
                logger.debug(f"Making request to {url} (attempt {attempt + 1})")
                
                response = self.session.get(
                    url,
                    timeout=self.config.REQUEST_TIMEOUT,
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"⏱️  Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"⚠️  HTTP {response.status_code} for {url}")
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️  Request failed (attempt {attempt + 1}): {e}")
                
                if attempt < retries:
                    time.sleep(self.config.RETRY_DELAY)
                    
        logger.error(f"❌ All request attempts failed for {url}")
        return None
    
    def _find_download_url(self, html_content: str) -> Optional[str]:
        """
        Find the download URL for OI spurts Excel file from HTML content
        NSE typically provides download links in specific patterns
        """
        try:
            # Common patterns for NSE download links
            patterns = [
                r'href="([^"]*oi[_-]?spurts[^"]*\.xlsx?)"',  # Direct Excel links
                r'href="([^"]*download[^"]*oi[^"]*)"',  # Download links containing 'oi'
                r'"downloadUrl":"([^"]*)"',  # JSON download URL
                r'data-url="([^"]*oi[^"]*)"',  # Data URL attributes
                r'action="([^"]*download[^"]*)"'  # Form actions
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                if matches:
                    url = matches[0]
                    # Convert relative URLs to absolute
                    if url.startswith('/'):
                        url = urljoin(self.config.NSE_BASE_URL, url)
                    elif not url.startswith('http'):
                        url = urljoin(self.config.NSE_OI_SPURTS_URL, url)
                    
                    logger.info(f"📄 Found potential download URL: {url}")
                    return url
            
            # If no direct links found, try to find API endpoints
            api_patterns = [
                r'"apiUrl":"([^"]*oi[^"]*)"',
                r'api/([^"]*oi[^"]*)',
                r'data-api="([^"]*)"'
            ]
            
            for pattern in api_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                if matches:
                    url = matches[0]
                    if url.startswith('/'):
                        url = urljoin(self.config.NSE_BASE_URL, url)
                    
                    logger.info(f"🔗 Found API endpoint: {url}")
                    return url
            
            # Try alternative approach - look for NSE's common Excel download endpoints
            common_endpoints = [
                '/api/equity-stockIndices?csv=true',
                '/api/option-chain-indices?symbol=NIFTY',
                '/content/indices/ind_niftyoptions.csv',
                '/api/reports?archives=[{%22name%22:%22F&O%20-%20OI%20Spurts%22,%22type%22:%22archives%22,%22category%22:%22derivatives%22,%22section%22:%22equity%22}]'
            ]
            
            for endpoint in common_endpoints:
                test_url = urljoin(self.config.NSE_BASE_URL, endpoint)
                logger.debug(f"Testing endpoint: {test_url}")
                
                test_response = self._make_request(test_url, retries=1)
                if test_response and test_response.headers.get('content-type', '').startswith(('application/vnd', 'application/octet')):
                    logger.info(f"✅ Found working endpoint: {test_url}")
                    return test_url
            
            logger.warning("⚠️  No download URL found, will try fallback method")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error finding download URL: {e}")
            return None
    
    def _download_excel_file(self, url: str) -> Optional[bytes]:
        """Download Excel file from URL"""
        try:
            logger.info(f"⬇️  Downloading Excel file from: {url}")
            
            # Update headers for file download
            download_headers = self.session.headers.copy()
            download_headers.update({
                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin'
            })
            
            response = self.session.get(
                url,
                headers=download_headers,
                timeout=self.config.REQUEST_TIMEOUT * 2,  # Longer timeout for file download
                stream=True
            )
            
            if response.status_code != 200:
                logger.error(f"❌ Download failed with status {response.status_code}")
                return None
            
            # Check if response is actually an Excel file
            content_type = response.headers.get('content-type', '').lower()
            if not (content_type.startswith('application/vnd') or 
                    content_type.startswith('application/octet') or
                    content_type.startswith('application/excel')):
                logger.warning(f"⚠️  Unexpected content type: {content_type}")
                
                # Check if it's HTML (error page)
                content_preview = response.content[:1000].decode('utf-8', errors='ignore')
                if '<html' in content_preview.lower():
                    logger.error("❌ Received HTML instead of Excel file")
                    return None
            
            content = response.content
            file_size = len(content)
            
            if file_size < 1000:  # Suspiciously small Excel file
                logger.warning(f"⚠️  Excel file seems too small ({file_size} bytes)")
                return None
            
            logger.info(f"✅ Successfully downloaded Excel file ({file_size:,} bytes)")
            return content
            
        except Exception as e:
            logger.error(f"❌ Error downloading Excel file: {e}")
            return None
    
    def _save_excel_file(self, excel_data: bytes) -> Optional[str]:
        """Save Excel file to disk"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"oi_spurts_{timestamp}.xlsx"
            file_path = os.path.join(self.config.EXCEL_DIR, filename)
            
            with open(file_path, 'wb') as f:
                f.write(excel_data)
            
            logger.info(f"💾 Saved Excel file: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"❌ Error saving Excel file: {e}")
            return None
    
    def get_fallback_data(self) -> Optional[Dict]:
        """
        Fallback method to get data when direct scraping fails
        This attempts to use NSE's alternative APIs or endpoints
        """
        try:
            logger.info("🔄 Attempting fallback data collection...")
            
            # Try NSE's public APIs
            fallback_urls = [
                "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
                "https://www.nseindia.com/api/equity-stockIndices?csv=true",
                "https://www.nseindia.com/api/chart-databyindex?index=OPTIDXNIFTY",
            ]
            
            for url in fallback_urls:
                logger.info(f"Trying fallback URL: {url}")
                response = self._make_request(url)
                
                if response:
                    # Try to process as JSON first
                    try:
                        data = response.json()
                        if data and isinstance(data, dict):
                            # Convert to our format
                            return self._process_fallback_json(data, url)
                    except:
                        # Try to process as CSV/Excel
                        if len(response.content) > 1000:
                            file_path = self._save_fallback_file(response.content, url)
                            if file_path:
                                return {
                                    'success': True,
                                    'file_path': file_path,
                                    'timestamp': datetime.now().isoformat(),
                                    'size': len(response.content),
                                    'url': url,
                                    'source': 'fallback'
                                }
            
            logger.error("❌ All fallback methods failed")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error in fallback data collection: {e}")
            return None
    
    def _process_fallback_json(self, data: Dict, source_url: str) -> Optional[Dict]:
        """Process JSON data from fallback APIs"""
        try:
            # Create a mock Excel file from JSON data
            import pandas as pd
            
            # Extract relevant data based on API structure
            if 'records' in data and 'data' in data['records']:
                df_data = data['records']['data']
            elif isinstance(data, list):
                df_data = data
            else:
                df_data = [data]
            
            # Create DataFrame
            df = pd.DataFrame(df_data)
            
            # Save as Excel
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"oi_spurts_fallback_{timestamp}.xlsx"
            file_path = os.path.join(self.config.EXCEL_DIR, filename)
            
            df.to_excel(file_path, index=False)
            
            return {
                'success': True,
                'file_path': file_path,
                'timestamp': datetime.now().isoformat(),
                'size': os.path.getsize(file_path),
                'url': source_url,
                'source': 'fallback_json'
            }
            
        except Exception as e:
            logger.error(f"❌ Error processing fallback JSON: {e}")
            return None
    
    def _save_fallback_file(self, content: bytes, source_url: str) -> Optional[str]:
        """Save fallback content as file"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Determine file extension
            if source_url.endswith('.csv') or 'csv' in source_url:
                extension = '.csv'
            else:
                extension = '.xlsx'
            
            filename = f"oi_spurts_fallback_{timestamp}{extension}"
            file_path = os.path.join(self.config.EXCEL_DIR, filename)
            
            with open(file_path, 'wb') as f:
                f.write(content)
            
            logger.info(f"💾 Saved fallback file: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"❌ Error saving fallback file: {e}")
            return None
    
    def cleanup_old_files(self, max_files: int = None):
        """Clean up old Excel files to prevent disk space issues"""
        try:
            if max_files is None:
                max_files = self.config.MAX_DAILY_FILES
            
            excel_files = []
            for filename in os.listdir(self.config.EXCEL_DIR):
                if filename.endswith(('.xlsx', '.xls', '.csv')):
                    file_path = os.path.join(self.config.EXCEL_DIR, filename)
                    stat = os.stat(file_path)
                    excel_files.append((file_path, stat.st_mtime))
            
            # Sort by modification time (newest first)
            excel_files.sort(key=lambda x: x[1], reverse=True)
            
            # Remove excess files
            if len(excel_files) > max_files:
                files_to_remove = excel_files[max_files:]
                for file_path, _ in files_to_remove:
                    try:
                        os.remove(file_path)
                        logger.info(f"🗑️  Removed old file: {os.path.basename(file_path)}")
                    except Exception as e:
                        logger.error(f"❌ Error removing file {file_path}: {e}")
                        
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")
    
    def get_scraping_status(self) -> Dict:
        """Get current scraping status"""
        return {
            'last_scrape_time': self.last_scrape_time.isoformat() if self.last_scrape_time else None,
            'excel_files_count': len([f for f in os.listdir(self.config.EXCEL_DIR) 
                                     if f.endswith(('.xlsx', '.xls', '.csv'))]),
            'session_active': bool(self.session.cookies),
            'config_url': self.config.NSE_OI_SPURTS_URL
        }
